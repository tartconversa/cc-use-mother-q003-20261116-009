"""铁路新闻证据系统：Offline, deterministic walkthrough of the evidence publication workflow."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .domain import ProductError
from .quality import Quality
from .report import Reports
from .service import RailwayService


def main():
    with tempfile.TemporaryDirectory() as temporary:
        service = RailwayService(Path(temporary) / "rail.sqlite3")
        service.register_source({"source_id": "demo_publisher", "host": "example.org",
                                 "kinds": ["freight", "milestone"], "max_age_days": 30,
                                 "trust_rank": 10, "version": 1, "active": True})
        quote = "The synthetic route carried 342 tonnes through October."
        claim = {"project_key": "route_demo", "kind": "freight", "metric": "cumulative_cargo",
                 "period": "2026-10-31", "amount": "342", "unit": "tonne", "quote": quote}
        capture = {"source_id": "demo_publisher", "external_id": "story-1",
                   "url": "https://example.org/news/route-1",
                   "fetched_at": "2026-11-16T08:00:00Z",
                   "published_at": "2026-11-15T08:00:00Z",
                   "title": "Synthetic railway operations example",
                   "body": json.dumps({"title": "Synthetic railway operations example",
                                       "text": quote, "claims": [claim]}),
                   "media_type": "application/json", "metadata": {"origin": "local-synthetic"}}
        first = service.replay(capture)
        repeat = service.replay(capture)
        if not first["created"] or repeat["created"] or first["claim_ids"] != repeat["claim_ids"]:
            raise AssertionError("capture replay must be idempotent")
        claim_id = first["claim_ids"][0]
        review = service.review(claim_id, {"decision": "accepted",
                                           "reviewer": "demo_reviewer",
                                           "reason": "Quote matches the synthetic publisher text",
                                           "expected_version": 0})
        if review["scope"]["selected_claim"] != claim_id:
            raise AssertionError("accepted evidence was not selected")
        released = service.publish({"name": "route_demo_release", "project_key": "route_demo"})
        report = Reports(service).snapshot(released["snapshot_id"], "route_demo")
        quality = Quality(service).publication_readiness("route_demo")
        if report["selected_count"] != 1 or report["series"][0]["latest"] != "342":
            raise AssertionError("snapshot report lost selected evidence")
        if quality["ready"] is not True:
            raise AssertionError("reviewed route must be publishable")

        # 策略降级：来源不再可信，停用并收缩可采指标。存量证据必须重新定级。
        downgraded = service.register_source(
            {"source_id": "demo_publisher", "host": "example.org", "kinds": ["milestone"],
             "max_age_days": 30, "trust_rank": 10, "version": 2, "active": False},
            expected_version=1)
        regrade = downgraded["regrade"]
        if regrade["demoted_claim_ids"] != [claim_id] or not regrade["changed"]:
            raise AssertionError("policy downgrade must demote stored accepted evidence")
        if service.claim(claim_id)["state"] != "regrade":
            raise AssertionError("demoted claim must wait in the regrade queue")
        if set(service.claim(claim_id)["regrade_reasons"]) != {"source_disabled", "kind_not_allowed"}:
            raise AssertionError("demotion must record the violated policy reasons")
        if service.scopes("route_demo")[0]["selected"] is not None:
            raise AssertionError("affected scope must drop its selection")
        if Quality(service).publication_readiness("route_demo")["ready"]:
            raise AssertionError("release readiness must reflect the downgrade")
        try:
            service.publish({"name": "blocked_release", "project_key": "route_demo"})
        except ProductError as error:
            if error.code != "unresolved_scope":
                raise
        else:
            raise AssertionError("publish must reject scopes invalidated by policy changes")

        # 历史快照保持原样，并解释当时使用的策略版本。
        frozen = Reports(service).snapshot(released["snapshot_id"], "route_demo")
        if frozen["series"][0]["latest"] != "342" or frozen["provenance"][0]["policy_version"] != 1:
            raise AssertionError("historical snapshot must keep its policy explanation")
        current = Reports(service).current("route_demo")
        if current["selected_count"] != 0 or current["unresolved_count"] != 1:
            raise AssertionError("current report must reflect the regrade")

        # 重复提交同一策略内容：不产生新的重定级或重复审计。
        before_events = len(service.audit(0, 500))
        repeated = service.register_source(
            {"source_id": "demo_publisher", "host": "example.org", "kinds": ["milestone"],
             "max_age_days": 30, "trust_rank": 10, "version": 3, "active": False},
            expected_version=2)
        if repeated["regrade"]["changed"]:
            raise AssertionError("resubmitting an identical policy must be a no-op regrade")
        if len(service.audit(0, 500)) != before_events + 1:
            raise AssertionError("identical policy must not duplicate regrade audits")

        # 策略恢复：证据不静默回升，仍需人工重审后才能重新发布。
        service.register_source(
            {"source_id": "demo_publisher", "host": "example.org",
             "kinds": ["freight", "milestone"], "max_age_days": 30,
             "trust_rank": 10, "version": 4, "active": True},
            expected_version=3)
        restored_claim = service.claim(claim_id)
        if restored_claim["state"] != "regrade" or restored_claim["regrade_reasons"]:
            raise AssertionError("restored policy must not silently re-accept evidence")
        if service.scopes("route_demo")[0]["selected"] is not None:
            raise AssertionError("scope selection must wait for human re-review")
        queue = service.review_queue("route_demo", state="regrade")
        if [row["id"] for row in queue] != [claim_id]:
            raise AssertionError("regrade queue must list the claim awaiting review")
        scope_version = service.scopes("route_demo")[0]["version"]
        service.review(claim_id, {"decision": "accepted", "reviewer": "demo_reviewer",
                                  "reason": "Policy restored; quote re-verified against source",
                                  "expected_version": scope_version})
        second = service.publish({"name": "route_demo_release_2", "project_key": "route_demo"})
        diff = Reports(service).compare_snapshots(released["snapshot_id"], second["snapshot_id"])
        if diff["change_count"] != 1:
            raise AssertionError("release diff must show the re-accepted scope")
        after = diff["changed_scopes"][0]["after"]
        if after["policy_version"] != 4 or after["capture_policy_version"] != 4:
            raise AssertionError("new snapshot must explain the restored policy version")

        print(json.dumps({"capture_id": first["capture_id"], "idempotent_replay": True,
                          "claim_id": claim_id, "scope_version": review["scope"]["version"],
                          "snapshot_id": released["snapshot_id"], "digest": released["digest"],
                          "latest_tonnes": report["series"][0]["latest"],
                          "provenance_count": len(report["provenance"]), "ready": quality["ready"],
                          "regrade_demoted": regrade["demoted_claim_ids"],
                          "regrade_queue": [row["id"] for row in queue],
                          "no_silent_recovery": True, "no_duplicate_audit": True,
                          "second_snapshot_id": second["snapshot_id"],
                          "snapshot_policy_version": frozen["provenance"][0]["policy_version"],
                          "republished_policy_version": after["policy_version"]},
                         ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
