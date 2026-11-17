"""铁路新闻证据系统：Coordinates replay, extraction, review and reproducible evidence releases."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json

from .domain import (Capture, Claim, ProductError, SourcePolicy, claims_equivalent,
                     evidence_issues, fingerprint, select_evidence, token)
from .extract import Extractor
from .schedule import Schedule
from .store import Store


class RailwayService:
    def __init__(self, db_path):
        self.store = Store(db_path)
        self.schedule = Schedule(self.store)
        self.extractor = Extractor()

    def register_source(self, data, expected_version=None):
        policy = SourcePolicy.parse(data)
        return self.store.upsert_source(policy, expected_version)

    def policy(self, source_id):
        row = self.store.source(source_id)
        return SourcePolicy(row["source_id"], row["host"], tuple(row["kinds"]),
                            row["max_age_days"], row["trust_rank"], row["version"], row["active"])

    def replay(self, data):
        capture = Capture.parse(data)
        attempts = 3
        for attempt in range(attempts):
            try:
                return self._replay_once(capture)
            except ProductError as error:
                if error.code != "policy_changed" or attempt + 1 == attempts:
                    raise
        raise ProductError("policy_changed", "source policy changed during replay", 409)

    def _replay_once(self, capture):
        policy = self.policy(capture.source_id)
        capture.check(policy)
        existing = self.store.capture_identity(capture.source_id, capture.external_id)
        same = next((row for row in existing if row["content_hash"] == capture.content_hash()), None)
        if same and same["state"] != "received":
            return {"capture_id": same["id"], "created": False, "state": same["state"],
                    "claim_ids": [c["id"] for c in self.store.claims_for_capture(same["id"])], "issues": []}
        claims, issues = self.extractor.inspect(capture, policy)
        if same:
            capture_id, created = same["id"], False
        else:
            capture_id, created = self.store.put_capture(capture, policy)
        if issues:
            self.store.set_capture_state(capture_id, "received", "quarantined", {"issues": issues})
            return {"capture_id": capture_id, "created": created, "state": "quarantined", "claim_ids": [], "issues": issues}
        claim_ids = self.store.insert_claims(capture_id, claims, policy.version)
        return {"capture_id": capture_id, "created": created, "state": "parsed", "claim_ids": claim_ids, "issues": []}

    @staticmethod
    def _decode_reasons(row):
        if row.get("regrade_reasons"):
            row["regrade_reasons"] = json.loads(row["regrade_reasons"])
        return row

    def capture(self, capture_id):
        row = self.store.capture(capture_id)
        row["claims"] = [self._decode_reasons(claim) for claim in self.store.claims_for_capture(capture_id)]
        return row

    def review_queue(self, project_key=None, state="proposed", limit=100):
        if state not in {"proposed", "accepted", "rejected", "hold", "retracted", "regrade"}:
            raise ProductError("invalid_state", "unsupported review state")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_limit", "limit must be 1..500")
        if project_key:
            project_key = token(project_key, "project_key")
            rows = self.store.all("SELECT c.*,a.source_id,a.url,a.published_at FROM claims c JOIN captures a ON a.id=c.capture_id WHERE c.state=? AND c.project_key=? ORDER BY c.id LIMIT ?",
                                  (state, project_key, limit))
        else:
            rows = self.store.all("SELECT c.*,a.source_id,a.url,a.published_at FROM claims c JOIN captures a ON a.id=c.capture_id WHERE c.state=? ORDER BY c.id LIMIT ?",
                                  (state, limit))
        return [self._decode_reasons(row) for row in rows]

    def claim(self, claim_id):
        row = self._decode_reasons(self.store.claim(claim_id))
        row["capture"] = self.store.capture(row["capture_id"])
        row["scope"] = self.store.scope(self._key(row))
        return row

    @staticmethod
    def _key(row):
        return (row["project_key"], row["kind"], row["metric"], row["period"])

    @staticmethod
    def _same(a, b):
        return claims_equivalent(a, b)

    def compare(self, claim_id):
        claim = self.store.claim(claim_id)
        key = self._key(claim)
        candidates = self.store.candidates(key)
        scope = self.store.scope(key)
        accepted = [c for c in candidates if c["state"] == "accepted" and c["capture_state"] == "parsed"]
        groups = []
        for candidate in accepted:
            group = next((g for g in groups if self._same(g[0], candidate)), None)
            if group is None:
                groups.append([candidate])
            else:
                group.append(candidate)
        groups.sort(key=lambda group: (-min(c["trust_rank"] for c in group), -len(group), min(c["id"] for c in group)))
        return {"key": key, "scope": scope, "candidate_count": len(candidates),
                "accepted_count": len(accepted), "value_groups": [[c["id"] for c in group] for group in groups],
                "conflict": len(groups) > 1,
                "suggested_claim": groups[0][0]["id"] if groups else None}

    def review(self, claim_id, data):
        claim = self.store.claim(claim_id)
        if claim["state"] not in {"proposed", "regrade"}:
            raise ProductError("already_reviewed", "claim has already been reviewed", 409)
        decision = data.get("decision")
        if decision not in {"accepted", "rejected", "hold"}:
            raise ProductError("invalid_decision", "decision must be accepted, rejected or hold")
        reviewer = token(data.get("reviewer"), "reviewer")
        reason = data.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ProductError("invalid_reason", "review requires a short reason")
        expected = data.get("expected_version")
        if type(expected) is not int or expected < 0:
            raise ProductError("invalid_version", "expected_version must be nonnegative")
        capture = self.store.capture(claim["capture_id"])
        if capture["state"] != "parsed":
            raise ProductError("capture_state", "claim capture is not parsed", 409)
        policy = self.store.source(capture["source_id"])
        if decision == "accepted":
            issues = evidence_issues(policy, capture, claim)
            if issues:
                raise ProductError("policy_mismatch",
                                   "claim conflicts with current source policy: " + ",".join(issues), 409)
        key = self._key(claim)
        candidates = self.store.candidates(key)
        accepted = [c for c in candidates if c["state"] == "accepted" and c["capture_state"] == "parsed"]
        if decision == "accepted":
            accepted.append({**claim, "trust_rank": policy["trust_rank"]})
        selected, conflict = select_evidence(accepted)
        scope = self.store.record_review(claim_id, decision, reviewer, reason.strip(), expected, selected, conflict)
        return {"claim_id": claim_id, "decision": decision, "scope": scope}

    def _selection(self, accepted):
        return select_evidence(accepted)

    def scopes(self, project_key=None, include_empty=True):
        if project_key:
            project_key = token(project_key, "project_key")
            rows = self.store.all("SELECT * FROM scopes WHERE project_key=? ORDER BY kind,metric,period", (project_key,))
        else:
            rows = self.store.all("SELECT * FROM scopes ORDER BY project_key,kind,metric,period")
        for row in rows:
            if row["selected_claim"]:
                claim = self.store.claim(row["selected_claim"])
                capture = self.store.capture(claim["capture_id"])
                policy = self.store.source(capture["source_id"])
                row["selected"] = {"claim_id": claim["id"], "amount": claim["amount"],
                                   "unit": claim["unit"], "milestone": claim["milestone"],
                                   "quote": claim["quote"], "source_id": capture["source_id"],
                                   "url": capture["url"], "published_at": capture["published_at"],
                                   "trust_rank": policy["trust_rank"],
                                   "policy_version": policy["version"],
                                   "capture_policy_version": capture["policy_version"]}
            else:
                row["selected"] = None
        return rows if include_empty else [row for row in rows if row["selected"]]

    def stale_selections(self, project_key=None):
        """铁路新闻证据系统：Selected claims that no longer satisfy the current source policy."""
        rows = []
        for scope in self.scopes(project_key):
            selected = scope["selected"]
            if not selected:
                continue
            claim = self.store.claim(selected["claim_id"])
            capture = self.store.capture(claim["capture_id"])
            policy = self.store.source(capture["source_id"])
            reasons = []
            if claim["state"] != "accepted":
                reasons.append("claim_not_accepted")
            if capture["state"] != "parsed":
                reasons.append("capture_not_current")
            if capture["policy_version"] != policy["version"]:
                reasons.append("policy_changed")
            reasons += evidence_issues(policy, capture, claim)
            if reasons:
                rows.append({"claim_id": claim["id"],
                             "scope": {"project_key": scope["project_key"], "kind": scope["kind"],
                                       "metric": scope["metric"], "period": scope["period"]},
                             "reasons": reasons, "policy_version": policy["version"],
                             "capture_policy_version": capture["policy_version"]})
        return rows

    def publish(self, data):
        name = token(data.get("name"), "name")
        project_key = data.get("project_key")
        scopes = self.scopes(project_key)
        if not scopes:
            raise ProductError("empty_release", "no reviewed scopes to publish", 409)
        blocked = [row for row in scopes if row["conflict"] or row["selected"] is None]
        if blocked:
            raise ProductError("unresolved_scope", f"{len(blocked)} scopes are unresolved", 409)
        stale = self.stale_selections(project_key)
        if stale:
            raise ProductError("stale_scope",
                               f"{len(stale)} scopes rely on evidence outdated by policy changes", 409)
        row, created = self.store.save_snapshot(
            name, scopes, validate=lambda db: self._validate_snapshot(db, scopes))
        return {"snapshot_id": row["id"], "digest": row["digest"], "created": created,
                "scope_count": row["scope_count"], "claim_count": row["claim_count"]}

    @staticmethod
    def _validate_snapshot(db, scopes):
        """铁路新闻证据系统：Re-check selection freshness inside the snapshot transaction."""
        for scope in scopes:
            selected = scope["selected"]
            row = db.execute(
                "SELECT c.state AS claim_state,c.kind,a.state AS capture_state,a.url,a.fetched_at,"
                "a.published_at,s.active,s.kinds_json,s.max_age_days,s.host FROM claims c "
                "JOIN captures a ON a.id=c.capture_id JOIN sources s ON s.source_id=a.source_id "
                "WHERE c.id=?", (selected["claim_id"],)).fetchone()
            if row is None or row["claim_state"] != "accepted" or row["capture_state"] != "parsed":
                raise ProductError("stale_scope", "selected evidence changed during publication", 409)
            policy = {"active": bool(row["active"]), "kinds": json.loads(row["kinds_json"]),
                      "max_age_days": row["max_age_days"], "host": row["host"]}
            capture = {"url": row["url"], "fetched_at": row["fetched_at"],
                       "published_at": row["published_at"]}
            if evidence_issues(policy, capture, {"kind": row["kind"]}):
                raise ProductError("stale_scope", "selected evidence conflicts with current policy", 409)

    def snapshot(self, snapshot_id):
        return self.store.snapshot(snapshot_id)

    def audit(self, since=0, limit=100):
        if type(since) is not int or since < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_page", "invalid audit pagination")
        return self.store.audit_events(since, limit)
