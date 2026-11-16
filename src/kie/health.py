"""Deterministic runtime health supervision for the installed KOR engines.

This module observes Hermes plugin registration and KIE's bounded event ledger.
It does not probe by running model calls, execute commands, or infer that an idle
engine has failed merely because it has not emitted an event recently.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .event_store import SQLiteEventStore

EXPECTED_PLUGINS = {
    "evidence-engine": "Evidence Engine",
    "critical-thinking-engine": "Critical Thinking Engine",
    "reliability-engine": "Reliability Engine",
    "emotion-engine": "Emotion Engine",
}
_EVENT_WINDOW = timedelta(minutes=15)
_MAX_EVENTS_TO_INSPECT = 100


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HealthSupervisor:
    """Read-only health observer and deduplicated pre-LLM alert producer."""

    def __init__(
        self,
        ctx: Any,
        *,
        kie_database: str | Path,
        event_server: Any,
        evidence_python: str | Path,
        evidence_source_root: str | Path,
        evidence_database: str | Path,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.ctx = ctx
        self.kie_database = Path(kie_database).expanduser()
        self.event_server = event_server
        self.evidence_python = Path(evidence_python).expanduser()
        self.evidence_source_root = Path(evidence_source_root).expanduser()
        self.evidence_database = Path(evidence_database).expanduser()
        self._now = now or _utc_now
        self._last_fingerprint: tuple[tuple[str, str], ...] | None = None

    @staticmethod
    def _issue(component: str, code: str, severity: str, summary: str) -> dict[str, str]:
        return {
            "component": component,
            "code": code,
            "severity": severity,
            "summary": summary,
        }

    def _plugin_status(self) -> tuple[dict[str, str], list[dict[str, str]]]:
        states: dict[str, str] = {}
        issues: list[dict[str, str]] = []
        probe = getattr(self.ctx, "has_plugin", None)
        if not callable(probe):
            return (
                {plugin_id: "unknown" for plugin_id in EXPECTED_PLUGINS},
                [self._issue(
                    "hermes-plugin-registry",
                    "plugin_probe_unavailable",
                    "critical",
                    "Hermes does not expose the plugin-presence probe required by KIE.",
                )],
            )
        for plugin_id, label in EXPECTED_PLUGINS.items():
            try:
                loaded = bool(probe(plugin_id))
            except Exception:
                loaded = False
                state = "probe_failed"
            else:
                state = "loaded" if loaded else "missing_or_disabled"
            states[plugin_id] = state
            if not loaded:
                issues.append(self._issue(
                    plugin_id,
                    "plugin_missing_or_disabled",
                    "warning",
                    f"{label} is not loaded and enabled in this Hermes profile.",
                ))
        return states, issues

    def _event_ingress_status(self) -> tuple[str, list[dict[str, str]]]:
        running = getattr(self.event_server, "is_running", False)
        if callable(running):
            try:
                running = running()
            except Exception:
                running = False
        if running is True:
            return "ready", []
        return "unavailable", [self._issue(
            "kie-event-ingress",
            "event_socket_not_running",
            "critical",
            "KIE's local engine-event receiver is not running.",
        )]

    def _evidence_adapter_status(self) -> tuple[dict[str, str], list[dict[str, str]]]:
        details: dict[str, str] = {}
        issues: list[dict[str, str]] = []
        details["python"] = "ready" if self.evidence_python.is_file() else "unavailable"
        if details["python"] != "ready":
            issues.append(self._issue(
                "evidence-engine",
                "evidence_interpreter_unavailable",
                "critical",
                "The configured Python interpreter for the Evidence bridge is unavailable.",
            ))

        bridge = self.evidence_source_root / "src" / "evidence_engine" / "bridge.py"
        details["source"] = "ready" if bridge.is_file() else "unavailable"
        if details["source"] != "ready":
            issues.append(self._issue(
                "evidence-engine",
                "evidence_bridge_unavailable",
                "critical",
                "The installed Evidence Engine bridge source could not be found.",
            ))

        # A fresh profile has no Evidence database until the first coding task.
        # Its absence is an uninitialized state, not an engine failure.
        if self.evidence_database.is_file():
            details["database"] = "ready"
        else:
            details["database"] = "not_initialized"
        return details, issues

    def _recent_event_issues(self, now: datetime) -> tuple[str, list[dict[str, str]]]:
        if not self.kie_database.is_file():
            return "not_initialized", []
        try:
            with SQLiteEventStore(self.kie_database) as store:
                records = store.read_recent_events(limit=_MAX_EVENTS_TO_INSPECT)
        except Exception:
            return "unavailable", [self._issue(
                "kie-event-store",
                "event_store_unavailable",
                "critical",
                "KIE could not read its event ledger.",
            )]

        issues: dict[tuple[str, str], dict[str, str]] = {}
        for record in records:
            event = record.event
            age = now - event.recorded_at
            if age < timedelta(seconds=-60) or age > _EVENT_WINDOW:
                continue
            payload = event.payload
            if event.event_type == "reliability.runtime_observation":
                kind = payload.get("kind")
                if kind == "provider_error":
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_provider_error",
                        "warning",
                        "Reliability reported a provider/API error in the current observation window.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
                elif kind == "response_anomaly":
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_response_anomaly",
                        "warning",
                        "Reliability detected an allowlisted response anomaly.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
            elif event.event_type == "reliability.runtime_snapshot":
                pressure = payload.get("context_pressure")
                if pressure == "critical":
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_context_pressure_critical",
                        "critical",
                        "Reliability reported critical context pressure in its latest session snapshot.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
                elif pressure == "warning":
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_context_pressure_warning",
                        "warning",
                        "Reliability reported elevated context pressure in its latest session snapshot.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
                if payload.get("api_errors", 0) > 0:
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_api_errors",
                        "warning",
                        "Reliability reported API/provider errors in its latest session snapshot.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
                if payload.get("anomaly_count", 0) > 0:
                    issue = self._issue(
                        "reliability-engine",
                        "reliability_response_anomalies",
                        "warning",
                        "Reliability reported response anomalies in its latest session snapshot.",
                    )
                    issues[(issue["component"], issue["code"])] = issue
            elif event.event_type == "critical_thinking.hook_failure":
                issue = self._issue(
                    "critical-thinking-engine",
                    "critical_thinking_hook_failure",
                    "warning",
                    "Critical Thinking reported that its review hook encountered an internal failure.",
                )
                issues[(issue["component"], issue["code"])] = issue
            elif event.event_type == "emotion.reflection_failed":
                issue = self._issue(
                    "emotion-engine",
                    "emotion_reflection_failure",
                    "warning",
                    "Emotion reported a reflection-handler failure.",
                )
                issues[(issue["component"], issue["code"])] = issue
            elif event.event_type == "evidence.verification_unavailable":
                issue = self._issue(
                    "evidence-engine",
                    "evidence_verification_unavailable",
                    "warning",
                    "Evidence could not run a requested coding verification.",
                )
                issues[(issue["component"], issue["code"])] = issue
        return "ready", list(issues.values())

    def get_report(self) -> dict[str, Any]:
        """Return a bounded, content-free snapshot. This method never creates a database."""
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("health clock must return a timezone-aware datetime")

        plugins, issues = self._plugin_status()
        ingress_status, ingress_issues = self._event_ingress_status()
        issues.extend(ingress_issues)
        evidence, evidence_issues = self._evidence_adapter_status()
        issues.extend(evidence_issues)
        event_store, event_issues = self._recent_event_issues(now)
        issues.extend(event_issues)

        return {
            "status": "degraded" if issues else "healthy",
            "checked_at": now.isoformat(),
            "plugins": plugins,
            "event_ingress": ingress_status,
            "event_store": event_store,
            "evidence_adapter": evidence,
            "active_issue_count": len(issues),
            "issues": sorted(
                issues,
                key=lambda item: (
                    {"critical": 0, "warning": 1, "info": 2}.get(item["severity"], 3),
                    item["component"],
                    item["code"],
                ),
            ),
        }

    def on_pre_llm_call(self, **kwargs: Any) -> dict[str, str] | None:
        """Inject one alert when health changes; no extra LLM call is initiated."""
        del kwargs
        try:
            report = self.get_report()
        except Exception:
            failure_fingerprint = (("kie-health-supervisor", "inspection_failed"),)
            if failure_fingerprint == self._last_fingerprint:
                return None
            self._last_fingerprint = failure_fingerprint
            return {
                "context": (
                    "[KIE HEALTH SUPERVISION WARNING]\n"
                    "KIE could not complete its health inspection. Do not assume every "
                    "engine or integration is available; report this uncertainty plainly."
                )
            }

        issues = report["issues"]
        fingerprint = tuple(sorted((item["component"], item["code"]) for item in issues))
        previous = self._last_fingerprint
        if fingerprint == previous:
            return None
        self._last_fingerprint = fingerprint

        if not issues:
            if previous:
                return {
                    "context": (
                        "[KIE HEALTH RECOVERED]\n"
                        "The previously reported KOR engine health issues are no longer "
                        "present in the current inspection. This confirms the observed "
                        "checks recovered; it is not a guarantee of future operation."
                    )
                }
            return None

        lines = [
            "[KIE SYSTEM HEALTH ALERT]",
            f"Current state: {report['status'].upper()}",
            "KIE detected the following issues from plugin state and bounded runtime observations:",
        ]
        for issue in issues:
            lines.append(
                f"- [{issue['severity'].upper()}] {issue['component']}: {issue['summary']}"
            )
        lines.append(
            "Treat these as technical observations, not proof of data loss or task failure. "
            "Do not claim the affected engine completed work successfully. Inform the user "
            "when the issue affects the requested task."
        )
        return {"context": "\n".join(lines)}


__all__ = ["EXPECTED_PLUGINS", "HealthSupervisor"]
