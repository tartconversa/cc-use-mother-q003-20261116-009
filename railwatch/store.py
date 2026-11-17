"""铁路新闻证据系统：SQLite ledger for sources, captures, claims, review and publishable snapshots."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path

from .domain import ProductError, canonical, evidence_issues, fingerprint, select_evidence


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS sources (
 source_id TEXT PRIMARY KEY, host TEXT NOT NULL UNIQUE, kinds_json TEXT NOT NULL,
 max_age_days INTEGER NOT NULL, trust_rank INTEGER NOT NULL, version INTEGER NOT NULL,
 active INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS captures (
 id INTEGER PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
 external_id TEXT NOT NULL, url TEXT NOT NULL, fetched_at TEXT NOT NULL,
 published_at TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL,
 media_type TEXT NOT NULL, metadata_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 state TEXT NOT NULL, policy_version INTEGER NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(source_id,external_id,content_hash)
);
CREATE INDEX IF NOT EXISTS captures_identity ON captures(source_id,external_id,id);
CREATE TABLE IF NOT EXISTS claims (
 id INTEGER PRIMARY KEY, capture_id INTEGER NOT NULL REFERENCES captures(id),
 project_key TEXT NOT NULL, kind TEXT NOT NULL, metric TEXT NOT NULL,
 period TEXT NOT NULL, amount TEXT, unit TEXT, milestone TEXT, quote TEXT NOT NULL,
 claim_hash TEXT NOT NULL, state TEXT NOT NULL, regrade_reasons TEXT, created_at TEXT NOT NULL,
 UNIQUE(capture_id,claim_hash)
);
CREATE INDEX IF NOT EXISTS claims_scope ON claims(project_key,kind,metric,period);
CREATE TABLE IF NOT EXISTS reviews (
 id INTEGER PRIMARY KEY, claim_id INTEGER NOT NULL REFERENCES claims(id),
 decision TEXT NOT NULL, reviewer TEXT NOT NULL, reason TEXT NOT NULL,
 expected_version INTEGER NOT NULL, resulting_version INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scopes (
 project_key TEXT NOT NULL, kind TEXT NOT NULL, metric TEXT NOT NULL,
 period TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0,
 selected_claim INTEGER REFERENCES claims(id), conflict INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL, PRIMARY KEY(project_key,kind,metric,period)
);
CREATE TABLE IF NOT EXISTS snapshots (
 id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL,
 digest TEXT NOT NULL UNIQUE, scope_count INTEGER NOT NULL, claim_count INTEGER NOT NULL,
 content_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
 id INTEGER PRIMARY KEY, event TEXT NOT NULL, subject TEXT NOT NULL,
 detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
"""


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    def initialize(self):
        with closing(self.connect()) as db:
            db.executescript(SCHEMA)
            columns = [row["name"] for row in db.execute("PRAGMA table_info(claims)")]
            if "regrade_reasons" not in columns:
                db.execute("ALTER TABLE claims ADD COLUMN regrade_reasons TEXT")

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def one(self, query, args=()):
        with closing(self.connect()) as db:
            row = db.execute(query, args).fetchone()
        return dict(row) if row else None

    def all(self, query, args=()):
        with closing(self.connect()) as db:
            rows = db.execute(query, args).fetchall()
        return [dict(row) for row in rows]

    def audit(self, db, event, subject, detail):
        db.execute("INSERT INTO audit(event,subject,detail_json,created_at) VALUES(?,?,?,?)",
                   (event, subject, canonical(detail), now()))

    def source(self, source_id):
        row = self.one("SELECT * FROM sources WHERE source_id=?", (source_id,))
        if row is None:
            raise ProductError("unknown_source", "source is not registered", 404)
        row["kinds"] = json.loads(row.pop("kinds_json"))
        row["active"] = bool(row["active"])
        return row

    @staticmethod
    def _regrade_summary():
        return {"changed": False, "demoted_claim_ids": [], "valid_claim_ids": [],
                "pending_regrade_claim_ids": [], "updated_regrade_claim_ids": [],
                "recertified_capture_ids": [], "scopes_regraded": []}

    def upsert_source(self, policy, expected_version=None):
        summary = self._regrade_summary()
        with self.transaction() as db:
            old = db.execute("SELECT * FROM sources WHERE source_id=?", (policy.source_id,)).fetchone()
            stamp = now()
            if old:
                if expected_version != old["version"]:
                    raise ProductError("stale_version", "source policy version changed", 409)
                if policy.version != old["version"] + 1:
                    raise ProductError("invalid_version", "policy version must increase by one", 409)
                db.execute("UPDATE sources SET host=?,kinds_json=?,max_age_days=?,trust_rank=?,version=?,active=?,updated_at=? WHERE source_id=?",
                           (policy.host, canonical(policy.kinds), policy.max_age_days, policy.trust_rank,
                            policy.version, int(policy.active), stamp, policy.source_id))
                self.audit(db, "source_updated", policy.source_id, {"old_version": old["version"], "new_version": policy.version})
                effective_change = (bool(old["active"]) != policy.active or old["host"] != policy.host
                                    or set(json.loads(old["kinds_json"])) != set(policy.kinds)
                                    or old["max_age_days"] != policy.max_age_days
                                    or old["trust_rank"] != policy.trust_rank)
                if effective_change:
                    summary = self._regrade_source(db, policy)
                    summary["changed"] = True
            else:
                if expected_version is not None or policy.version != 1:
                    raise ProductError("invalid_version", "new source starts at version one", 409)
                db.execute("INSERT INTO sources VALUES(?,?,?,?,?,?,?,?,?)",
                           (policy.source_id, policy.host, canonical(policy.kinds), policy.max_age_days,
                            policy.trust_rank, policy.version, int(policy.active), stamp, stamp))
                self.audit(db, "source_created", policy.source_id, {"version": policy.version})
        row = self.source(policy.source_id)
        row["regrade"] = summary
        return row

    def _regrade_source(self, db, policy):
        """铁路新闻证据系统：Re-grade stored evidence inside the policy update transaction.

        Accepted claims that violate the new policy are demoted to the regrade
        queue; scopes touching this source are recomputed. A claim that already
        sits in regrade is never silently restored: only a human review can
        accept it again. Rows whose outcome does not change are left untouched
        so repeated equivalent updates produce no duplicate audits.
        """
        summary = self._regrade_summary()
        view = {"active": policy.active, "host": policy.host,
                "kinds": list(policy.kinds), "max_age_days": policy.max_age_days}
        captures = db.execute("SELECT * FROM captures WHERE source_id=? AND state!='superseded' ORDER BY id",
                              (policy.source_id,)).fetchall()
        for capture in captures:
            reasons = evidence_issues(view, dict(capture))
            if not reasons and capture["policy_version"] != policy.version:
                db.execute("UPDATE captures SET policy_version=? WHERE id=?", (policy.version, capture["id"]))
                summary["recertified_capture_ids"].append(capture["id"])
        claims = db.execute(
            "SELECT c.*,a.url,a.fetched_at,a.published_at FROM claims c JOIN captures a ON a.id=c.capture_id "
            "WHERE a.source_id=? AND a.state='parsed' ORDER BY c.id", (policy.source_id,)).fetchall()
        keys = set()
        for claim in claims:
            keys.add((claim["project_key"], claim["kind"], claim["metric"], claim["period"]))
            capture_view = {"url": claim["url"], "fetched_at": claim["fetched_at"],
                            "published_at": claim["published_at"]}
            reasons = evidence_issues(view, capture_view, dict(claim))
            if claim["state"] == "accepted":
                if reasons:
                    db.execute("UPDATE claims SET state='regrade',regrade_reasons=? WHERE id=?",
                               (canonical(reasons), claim["id"]))
                    self.audit(db, "claim_regraded", str(claim["id"]),
                               {"from": "accepted", "reasons": reasons, "policy_version": policy.version})
                    summary["demoted_claim_ids"].append(claim["id"])
                else:
                    summary["valid_claim_ids"].append(claim["id"])
            elif claim["state"] == "regrade":
                summary["pending_regrade_claim_ids"].append(claim["id"])
                stored = json.loads(claim["regrade_reasons"]) if claim["regrade_reasons"] else []
                if reasons != stored:
                    db.execute("UPDATE claims SET regrade_reasons=? WHERE id=?",
                               (canonical(reasons), claim["id"]))
                    self.audit(db, "claim_regraded", str(claim["id"]),
                               {"from": "regrade", "reasons": reasons, "policy_version": policy.version})
                    summary["updated_regrade_claim_ids"].append(claim["id"])
        for key in sorted(keys):
            rows = db.execute(
                "SELECT c.id,c.kind,c.amount,c.unit,c.milestone,s.trust_rank FROM claims c "
                "JOIN captures a ON a.id=c.capture_id JOIN sources s ON s.source_id=a.source_id "
                "WHERE c.project_key=? AND c.kind=? AND c.metric=? AND c.period=? "
                "AND c.state='accepted' AND a.state='parsed' ORDER BY c.id", key).fetchall()
            selected, conflict = select_evidence([dict(row) for row in rows])
            current = db.execute("SELECT * FROM scopes WHERE project_key=? AND kind=? AND metric=? AND period=?",
                                 key).fetchone()
            if current is None:
                continue
            old_selected, old_conflict = current["selected_claim"], bool(current["conflict"])
            if old_selected != selected or old_conflict != conflict:
                db.execute("UPDATE scopes SET version=version+1,selected_claim=?,conflict=?,updated_at=? "
                           "WHERE project_key=? AND kind=? AND metric=? AND period=?",
                           (selected, int(conflict), now(), *key))
                self.audit(db, "scope_regraded", ":".join(key),
                           {"old_selected": old_selected, "new_selected": selected,
                            "conflict": conflict, "policy_version": policy.version})
                summary["scopes_regraded"].append(
                    {"project_key": key[0], "kind": key[1], "metric": key[2], "period": key[3],
                     "selected_claim": selected, "conflict": conflict})
        if (summary["demoted_claim_ids"] or summary["scopes_regraded"]
                or summary["recertified_capture_ids"] or summary["updated_regrade_claim_ids"]):
            self.audit(db, "source_regraded", policy.source_id,
                       {"policy_version": policy.version,
                        "demoted_claim_ids": summary["demoted_claim_ids"],
                        "updated_regrade_claim_ids": summary["updated_regrade_claim_ids"],
                        "recertified_capture_ids": summary["recertified_capture_ids"],
                        "scopes_regraded": len(summary["scopes_regraded"])})
        return summary

    def capture(self, capture_id):
        row = self.one("SELECT * FROM captures WHERE id=?", (capture_id,))
        if row is None:
            raise ProductError("unknown_capture", "capture does not exist", 404)
        row["metadata"] = json.loads(row.pop("metadata_json"))
        return row

    def capture_identity(self, source_id, external_id):
        return self.all("SELECT id,content_hash,state,policy_version FROM captures WHERE source_id=? AND external_id=? ORDER BY id",
                        (source_id, external_id))

    def put_capture(self, item, policy):
        with self.transaction() as db:
            current = db.execute("SELECT version FROM sources WHERE source_id=?", (item.source_id,)).fetchone()
            if current is None or current["version"] != policy.version:
                raise ProductError("policy_changed", "source policy changed during replay", 409)
            duplicate = db.execute("SELECT id FROM captures WHERE source_id=? AND external_id=? AND content_hash=?",
                                   (item.source_id, item.external_id, item.content_hash())).fetchone()
            if duplicate:
                return int(duplicate["id"]), False
            old_ids = [row["id"] for row in db.execute(
                "SELECT id FROM captures WHERE source_id=? AND external_id=? AND state!='superseded'",
                (item.source_id, item.external_id)).fetchall()]
            for old_id in old_ids:
                affected = db.execute("SELECT id FROM claims WHERE capture_id=?", (old_id,)).fetchall()
                for claim in affected:
                    db.execute("UPDATE scopes SET selected_claim=NULL,conflict=1,version=version+1,updated_at=? WHERE selected_claim=?",
                               (now(), claim["id"]))
                db.execute("UPDATE claims SET state='retracted' WHERE capture_id=? AND state IN ('accepted','regrade')", (old_id,))
            db.execute("UPDATE captures SET state='superseded' WHERE source_id=? AND external_id=? AND state!='superseded'",
                       (item.source_id, item.external_id))
            cursor = db.execute("INSERT INTO captures(source_id,external_id,url,fetched_at,published_at,title,body,media_type,metadata_json,content_hash,state,policy_version,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (item.source_id, item.external_id, item.url, item.fetched_at, item.published_at,
                                 item.title, item.body, item.media_type, canonical(item.metadata), item.content_hash(),
                                 "received", policy.version, now()))
            capture_id = cursor.lastrowid
            self.audit(db, "capture_received", str(capture_id), {"identity": item.identity(), "hash": item.content_hash()})
            if old_ids:
                self.audit(db, "capture_superseded", str(capture_id), {"old_capture_ids": old_ids})
            return capture_id, True

    def set_capture_state(self, capture_id, expected, target, detail):
        with self.transaction() as db:
            result = db.execute("UPDATE captures SET state=? WHERE id=? AND state=?", (target, capture_id, expected))
            if result.rowcount != 1:
                raise ProductError("capture_state", "capture state changed", 409)
            self.audit(db, "capture_" + target, str(capture_id), detail)

    def insert_claims(self, capture_id, claims, expected_version=None):
        inserted = []
        with self.transaction() as db:
            row = db.execute("SELECT state,policy_version FROM captures WHERE id=?", (capture_id,)).fetchone()
            if row is None or row["state"] != "received":
                raise ProductError("capture_state", "capture is not ready for extraction", 409)
            if expected_version is not None:
                current = db.execute("SELECT s.version FROM sources s JOIN captures a ON a.source_id=s.source_id WHERE a.id=?",
                                     (capture_id,)).fetchone()
                if current is None or current["version"] != expected_version:
                    raise ProductError("policy_changed", "source policy changed during replay", 409)
            for claim in claims:
                cursor = db.execute("INSERT INTO claims(capture_id,project_key,kind,metric,period,amount,unit,milestone,quote,claim_hash,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (capture_id, claim.project_key, claim.kind, claim.metric, claim.period,
                                     claim.amount, claim.unit, claim.milestone, claim.quote, claim.hash(), "proposed", now()))
                inserted.append(cursor.lastrowid)
            db.execute("UPDATE captures SET state='parsed' WHERE id=?", (capture_id,))
            self.audit(db, "capture_parsed", str(capture_id), {"claim_ids": inserted})
        return inserted

    def claim(self, claim_id):
        row = self.one("SELECT * FROM claims WHERE id=?", (claim_id,))
        if row is None:
            raise ProductError("unknown_claim", "claim does not exist", 404)
        return row

    def claims_for_capture(self, capture_id):
        return self.all("SELECT * FROM claims WHERE capture_id=? ORDER BY id", (capture_id,))

    def scope(self, key):
        row = self.one("SELECT * FROM scopes WHERE project_key=? AND kind=? AND metric=? AND period=?", key)
        if row is None:
            return {"project_key": key[0], "kind": key[1], "metric": key[2], "period": key[3],
                    "version": 0, "selected_claim": None, "conflict": 0}
        return row

    def candidates(self, key):
        return self.all("SELECT c.*,s.trust_rank,a.published_at,a.state AS capture_state FROM claims c JOIN captures a ON a.id=c.capture_id JOIN sources s ON s.source_id=a.source_id WHERE c.project_key=? AND c.kind=? AND c.metric=? AND c.period=? ORDER BY c.id", key)

    def record_review(self, claim_id, decision, reviewer, reason, expected_version, selected, conflict):
        claim = self.claim(claim_id)
        key = (claim["project_key"], claim["kind"], claim["metric"], claim["period"])
        with self.transaction() as db:
            current = db.execute("SELECT * FROM scopes WHERE project_key=? AND kind=? AND metric=? AND period=?", key).fetchone()
            version = current["version"] if current else 0
            if version != expected_version:
                raise ProductError("stale_version", "scope changed during review", 409)
            if current:
                db.execute("UPDATE scopes SET version=?,selected_claim=?,conflict=?,updated_at=? WHERE project_key=? AND kind=? AND metric=? AND period=?",
                           (version + 1, selected, int(conflict), now(), *key))
            else:
                db.execute("INSERT INTO scopes VALUES(?,?,?,?,?,?,?,?)",
                           (*key, 1, selected, int(conflict), now()))
            db.execute("UPDATE claims SET state=?,regrade_reasons=CASE WHEN ? IN ('accepted','rejected') THEN NULL ELSE regrade_reasons END WHERE id=?",
                       (decision, decision, claim_id))
            db.execute("INSERT INTO reviews(claim_id,decision,reviewer,reason,expected_version,resulting_version,created_at) VALUES(?,?,?,?,?,?,?)",
                       (claim_id, decision, reviewer, reason, version, version + 1, now()))
            self.audit(db, "claim_reviewed", str(claim_id), {"decision": decision, "scope_version": version + 1})
        return self.scope(key)

    def save_snapshot(self, name, scopes, validate=None):
        content = canonical(scopes)
        value = fingerprint(content)
        with self.transaction() as db:
            old = db.execute("SELECT * FROM snapshots WHERE digest=?", (value,)).fetchone()
            if old:
                return dict(old), False
            if validate is not None:
                validate(db)
            cursor = db.execute("INSERT INTO snapshots(name,created_at,digest,scope_count,claim_count,content_json) VALUES(?,?,?,?,?,?)",
                                (name, now(), value, len(scopes), sum(bool(s["selected_claim"]) for s in scopes), content))
            self.audit(db, "snapshot_created", str(cursor.lastrowid), {"digest": value, "name": name})
            return dict(db.execute("SELECT * FROM snapshots WHERE id=?", (cursor.lastrowid,)).fetchone()), True

    def snapshot(self, snapshot_id):
        row = self.one("SELECT * FROM snapshots WHERE id=?", (snapshot_id,))
        if row is None:
            raise ProductError("unknown_snapshot", "snapshot does not exist", 404)
        row["content"] = json.loads(row.pop("content_json"))
        return row

    def audit_events(self, since=0, limit=100):
        rows = self.all("SELECT * FROM audit WHERE id>? ORDER BY id LIMIT ?", (since, limit))
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json"))
        return rows
