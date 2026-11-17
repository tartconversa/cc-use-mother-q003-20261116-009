import json
import tempfile
import threading
import unittest
from pathlib import Path

from railwatch.domain import Capture, Claim, ProductError
from railwatch.quality import Quality
from railwatch.report import Reports
from railwatch.service import RailwayService


class RegradeTests(unittest.TestCase):
    """铁路新闻证据系统：Policy version changes drive re-grading of stored evidence."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = RailwayService(Path(self.temp.name) / "evidence.sqlite3")
        self.service.register_source(self.policy())

    def tearDown(self):
        self.temp.cleanup()

    def policy(self, source_id="news", host="example.org", rank=10, version=1,
               active=True, kinds=None, max_age_days=30):
        return {"source_id": source_id, "host": host,
                "kinds": kinds or ["freight", "milestone", "capacity"],
                "max_age_days": max_age_days, "trust_rank": rank,
                "version": version, "active": active}

    def claim(self, quantity="100", metric="cargo", project="rail-a", period="2026-11-15",
              kind="freight", milestone=None):
        data = {"project_key": project, "kind": kind, "metric": metric, "period": period,
                "quote": "Freight reached " + str(quantity) + " tonnes on the route."}
        if kind == "milestone":
            data["milestone"] = milestone or "testing"
        else:
            data["amount"] = str(quantity)
            data["unit"] = "tonne"
        return data

    def capture(self, source_id="news", external_id="article-1", claim=None, host="example.org",
                published_at="2026-11-15T12:00:00Z", fetched_at="2026-11-16T00:00:00Z"):
        claim = claim or self.claim()
        return {"source_id": source_id, "external_id": external_id,
                "url": "https://" + host + "/article/1", "fetched_at": fetched_at,
                "published_at": published_at, "title": "Railway article",
                "body": json.dumps({"title": "Railway article", "text": claim["quote"], "claims": [claim]}),
                "media_type": "application/json", "metadata": {"publisher": "synthetic"}}

    def review(self, claim_id, decision="accepted", expected=0, reviewer="analyst"):
        return self.service.review(claim_id, {"decision": decision, "reviewer": reviewer,
                                              "reason": "Verified quoted source evidence",
                                              "expected_version": expected})

    def accepted_claim(self, external_id="article-1", claim=None, **kwargs):
        claim_id = self.service.replay(self.capture(external_id=external_id, claim=claim, **kwargs))["claim_ids"][0]
        scope_version = self.service.scopes()[0]["version"] if self.service.scopes() else 0
        key_scopes = [s for s in self.service.scopes()
                      if (s["project_key"], s["kind"], s["metric"], s["period"])
                      == tuple(self.service.store.claim(claim_id)[k] for k in ("project_key", "kind", "metric", "period"))]
        version = key_scopes[0]["version"] if key_scopes else 0
        self.review(claim_id, expected=version)
        return claim_id

    def update_policy(self, expected, **changes):
        current = self.service.store.source("news")
        data = {"source_id": "news", "host": current["host"], "kinds": current["kinds"],
                "max_age_days": current["max_age_days"], "trust_rank": current["trust_rank"],
                "version": current["version"] + 1, "active": current["active"]}
        data.update(changes)
        return self.service.register_source(data, expected)

    def assert_code(self, code, action, *args):
        with self.assertRaises(ProductError) as caught:
            action(*args)
        self.assertEqual(caught.exception.code, code)

    def audit_events(self, name):
        return [event for event in self.service.audit(0, 500) if event["event"] == name]

    def test_disable_demotes_accepted_and_blocks_publish(self):
        claim_id = self.accepted_claim()
        self.service.publish({"name": "release-1"})
        result = self.update_policy(1, active=False)
        self.assertEqual(result["regrade"]["demoted_claim_ids"], [claim_id])
        claim = self.service.claim(claim_id)
        self.assertEqual(claim["state"], "regrade")
        self.assertEqual(claim["regrade_reasons"], ["source_disabled"])
        scope = self.service.scopes()[0]
        self.assertIsNone(scope["selected"])
        self.assertEqual(scope["version"], 2)
        queue = self.service.review_queue(state="regrade")
        self.assertEqual([row["id"] for row in queue], [claim_id])
        self.assertEqual(queue[0]["regrade_reasons"], ["source_disabled"])
        readiness = Quality(self.service).publication_readiness()
        self.assertFalse(readiness["ready"])
        self.assert_code("unresolved_scope", self.service.publish, {"name": "release-2"})

    def test_kind_removal_keeps_other_kinds_valid(self):
        freight_id = self.accepted_claim("article-1", self.claim(metric="cargo"))
        milestone_id = self.accepted_claim("article-2", self.claim(kind="milestone", metric="phase"))
        result = self.update_policy(1, kinds=["milestone", "capacity"])
        self.assertEqual(result["regrade"]["demoted_claim_ids"], [freight_id])
        self.assertEqual(result["regrade"]["valid_claim_ids"], [milestone_id])
        self.assertEqual(self.service.claim(freight_id)["state"], "regrade")
        self.assertEqual(self.service.claim(freight_id)["regrade_reasons"], ["kind_not_allowed"])
        self.assertEqual(self.service.claim(milestone_id)["state"], "accepted")
        scopes = {scope["kind"]: scope for scope in self.service.scopes()}
        self.assertIsNone(scopes["freight"]["selected"])
        self.assertEqual(scopes["milestone"]["selected_claim"], milestone_id)
        report = Reports(self.service).current("rail-a")
        self.assertEqual(report["selected_count"], 1)
        self.assertEqual(report["unresolved_count"], 1)
        self.assertEqual(report["milestones"][0]["latest"]["claim_id"], milestone_id)

    def test_trust_rank_change_reselects_across_sources(self):
        first = self.accepted_claim("article-1")
        self.service.register_source(self.policy("second", "second.org", rank=20))
        second = self.service.replay(self.capture("second", "article-2", host="second.org"))["claim_ids"][0]
        self.review(second, expected=1)
        self.assertEqual(self.service.scopes()[0]["selected_claim"], second)
        current = self.service.store.source("second")
        demoted = self.policy("second", "second.org", rank=5, version=2)
        result = self.service.register_source(demoted, current["version"])
        self.assertEqual(result["regrade"]["demoted_claim_ids"], [])
        scope = self.service.scopes()[0]
        self.assertEqual(scope["selected_claim"], first)
        self.assertEqual(scope["version"], 3)
        events = self.audit_events("scope_regraded")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["detail"]["new_selected"], first)

    def test_max_age_reduction_demotes_old_captures(self):
        claim_id = self.accepted_claim(claim=self.claim(period="2026-10-24"),
                                       published_at="2026-10-25T12:00:00Z",
                                       fetched_at="2026-11-16T00:00:00Z")
        result = self.update_policy(1, max_age_days=10)
        self.assertEqual(result["regrade"]["demoted_claim_ids"], [claim_id])
        self.assertEqual(self.service.claim(claim_id)["regrade_reasons"], ["stale_capture"])

    def test_duplicate_policy_has_no_side_effects(self):
        claim_id = self.accepted_claim()
        before_scope = self.service.scopes()[0]
        before_audits = len(self.service.audit(0, 500))
        result = self.update_policy(1)
        self.assertFalse(result["regrade"]["changed"])
        self.assertEqual(self.service.claim(claim_id)["state"], "accepted")
        after_scope = self.service.scopes()[0]
        self.assertEqual(after_scope["version"], before_scope["version"])
        self.assertEqual(after_scope["selected_claim"], before_scope["selected_claim"])
        self.assertEqual(after_scope["conflict"], before_scope["conflict"])
        self.assertEqual(after_scope["selected"]["capture_policy_version"], 1)
        after = self.service.audit(0, 500)
        self.assertEqual(len(after), before_audits + 1)
        self.assertEqual(after[-1]["event"], "source_updated")
        self.assertEqual(self.audit_events("claim_regraded"), [])
        self.assertEqual(self.audit_events("scope_regraded"), [])
        self.assertEqual(self.audit_events("source_regraded"), [])

    def test_restore_does_not_silently_recover(self):
        claim_id = self.accepted_claim()
        first_release = self.service.publish({"name": "release-1"})
        self.update_policy(1, active=False)
        restored = self.update_policy(2, active=True)
        self.assertEqual(restored["regrade"]["pending_regrade_claim_ids"], [claim_id])
        claim = self.service.claim(claim_id)
        self.assertEqual(claim["state"], "regrade")
        self.assertEqual(claim["regrade_reasons"], [])
        scope = self.service.scopes()[0]
        self.assertIsNone(scope["selected"])
        self.assert_code("unresolved_scope", self.service.publish, {"name": "release-2"})
        review = self.review(claim_id, expected=scope["version"])
        self.assertEqual(review["scope"]["selected_claim"], claim_id)
        self.assertEqual(self.service.claim(claim_id)["state"], "accepted")
        self.assertIsNone(self.service.claim(claim_id)["regrade_reasons"])
        second_release = self.service.publish({"name": "release-2"})
        diff = Reports(self.service).compare_snapshots(first_release["snapshot_id"],
                                                       second_release["snapshot_id"])
        self.assertEqual(diff["change_count"], 1)
        change = diff["changed_scopes"][0]
        self.assertEqual(change["before"]["policy_version"], 1)
        self.assertEqual(change["after"]["policy_version"], 3)

    def test_reaccept_of_still_invalid_claim_rejected(self):
        claim_id = self.accepted_claim()
        self.update_policy(1, active=False)
        scope_version = self.service.scopes()[0]["version"]
        self.assert_code("policy_mismatch", self.review, claim_id, "accepted", scope_version)
        self.assertEqual(self.service.claim(claim_id)["state"], "regrade")

    def test_proposed_claim_blocked_by_new_policy(self):
        claim_id = self.service.replay(self.capture())["claim_ids"][0]
        self.update_policy(1, kinds=["milestone", "capacity"])
        self.assertEqual(self.service.claim(claim_id)["state"], "proposed")
        self.assert_code("policy_mismatch", self.review, claim_id, "accepted", 0)
        result = self.review(claim_id, "rejected", expected=0)
        self.assertEqual(result["decision"], "rejected")

    def test_snapshot_frozen_and_explains_policy(self):
        claim_id = self.accepted_claim()
        release = self.service.publish({"name": "release-1"})
        snapshot = self.service.snapshot(release["snapshot_id"])
        selected = snapshot["content"][0]["selected"]
        self.assertEqual(selected["policy_version"], 1)
        self.assertEqual(selected["capture_policy_version"], 1)
        self.update_policy(1, active=False)
        again = self.service.snapshot(release["snapshot_id"])
        self.assertEqual(again["digest"], snapshot["digest"])
        self.assertEqual(again["content"], snapshot["content"])
        report = Reports(self.service).snapshot(release["snapshot_id"], "rail-a")
        self.assertEqual(report["series"][0]["latest"], "100")
        self.assertEqual(report["provenance"][0]["policy_version"], 1)
        current = Reports(self.service).current("rail-a")
        self.assertEqual(current["selected_count"], 0)
        self.assertEqual(current["unresolved_count"], 1)
        self.assert_code("unresolved_scope", self.service.publish, {"name": "release-2"})

    def test_repeated_downgrade_does_not_duplicate_audits(self):
        claim_id = self.accepted_claim()
        self.update_policy(1, active=False)
        self.assertEqual(len(self.audit_events("claim_regraded")), 1)
        self.assertEqual(len(self.audit_events("scope_regraded")), 1)
        self.assertEqual(len(self.audit_events("source_regraded")), 1)
        result = self.update_policy(2, active=False, trust_rank=9)
        self.assertTrue(result["regrade"]["changed"])
        self.assertEqual(result["regrade"]["demoted_claim_ids"], [])
        self.assertEqual(result["regrade"]["pending_regrade_claim_ids"], [claim_id])
        self.assertEqual(len(self.audit_events("claim_regraded")), 1)
        self.assertEqual(len(self.audit_events("scope_regraded")), 1)
        self.assertEqual(len(self.audit_events("source_regraded")), 1)
        self.assertEqual(self.service.claim(claim_id)["regrade_reasons"], ["source_disabled"])

    def test_regrade_reasons_refresh_when_policy_changes(self):
        claim_id = self.accepted_claim()
        self.update_policy(1, active=False)
        self.assertEqual(self.service.claim(claim_id)["regrade_reasons"], ["source_disabled"])
        self.update_policy(2, active=True, kinds=["milestone", "capacity"])
        self.assertEqual(self.service.claim(claim_id)["regrade_reasons"], ["kind_not_allowed"])
        self.assertEqual(len(self.audit_events("claim_regraded")), 2)

    def test_put_capture_rejects_stale_policy_version(self):
        stale = self.service.policy("news")
        self.update_policy(1, trust_rank=9)
        item = Capture.parse(self.capture())
        self.assert_code("policy_changed", self.service.store.put_capture, item, stale)

    def test_insert_claims_rejects_stale_policy_version(self):
        policy = self.service.policy("news")
        item = Capture.parse(self.capture())
        capture_id, created = self.service.store.put_capture(item, policy)
        self.assertTrue(created)
        self.update_policy(1, trust_rank=9)
        claims = [Claim.parse(self.claim())]
        self.assert_code("policy_changed", self.service.store.insert_claims,
                         capture_id, claims, policy.version)

    def test_rejected_regrade_claim_stays_rejected_after_restore(self):
        claim_id = self.accepted_claim()
        self.update_policy(1, active=False)
        scope_version = self.service.scopes()[0]["version"]
        self.review(claim_id, "rejected", expected=scope_version)
        self.assertEqual(self.service.claim(claim_id)["state"], "rejected")
        self.update_policy(2, active=True)
        self.assertEqual(self.service.claim(claim_id)["state"], "rejected")
        self.assert_code("already_reviewed", self.review, claim_id, "accepted",
                         self.service.scopes()[0]["version"])

    def test_concurrent_admin_and_replay(self):
        errors = []

        def admin():
            try:
                for version in range(2, 9):
                    self.service.register_source(
                        self.policy(version=version, active=version % 2 == 0), version - 1)
            except ProductError as exc:
                errors.append(("admin", exc.code))

        def replayer():
            for index in range(8):
                try:
                    self.service.replay(self.capture(external_id=f"article-{index}"))
                except ProductError as exc:
                    if exc.code not in {"source_disabled", "policy_changed"}:
                        errors.append(("replay", exc.code))

        threads = [threading.Thread(target=admin), threading.Thread(target=replayer)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        final = self.service.store.source("news")
        if not final["active"]:
            self.update_policy(final["version"], active=True)
        result = self.service.replay(self.capture(external_id="article-final"))
        self.assertEqual(result["state"], "parsed")
        for claim in self.service.review_queue(state="proposed", limit=500):
            self.assertIn(claim["kind"], self.service.store.source("news")["kinds"])


if __name__ == "__main__":
    unittest.main()
