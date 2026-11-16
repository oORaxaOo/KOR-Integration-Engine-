# Hermes integration — KIE v0.7

## What this adds

The KIE now includes a native Hermes Agent plugin that registers one asynchronous
read-only tool:

`kie_evidence_lookup(evidence_task_id)`

Hermes calls the plugin through its documented `ctx.register_tool(...,
is_async=True)` interface. The tool creates a short one-step KIE workflow,
records a host-originated lookup request, runs the existing Evidence adapter
through the controlled executor, and returns the minimized result event.

Hermes is the host/runtime, not a fifth domain engine. The event protocol
therefore recognizes the reserved producer identity `hermes_host`; the four
domain-engine IDs remain unchanged, and consumers remain restricted to those
four engines.

## Read-only stdio boundary

The plugin starts the existing Evidence Engine bridge using
`asyncio.create_subprocess_exec` with a fixed argv:

`<evidence_python> -m evidence_engine.bridge --db <evidence_database>`

No shell is invoked. The tool cannot choose the Python executable, source
directory, database path, bridge module, or RPC method. The transport invokes
only `get_task_report`, enforces a timeout and response-size bound, discards
stderr, and terminates a timed-out child. The Evidence Engine opens its
database through its existing read-only `EvidenceReader`.

The subprocess receives a minimal environment containing `PYTHONPATH`,
`PYTHONNOUSERSITE`, and `PYTHONUNBUFFERED`, plus only the dynamic-loader
variables `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` or Windows `SYSTEMROOT` when
present. Some Python distributions require these to locate their own runtime
libraries. Hermes credentials and the user's general environment are not
forwarded.

## Installation

This repository deliberately does not modify a user's Hermes installation or
configuration automatically. After reviewing and merging the KIE changes:

1. Install the KIE package into the Python environment used by Hermes:
   `<HERMES_PYTHON> -m pip install -e /absolute/path/to/KOR-Integration-Engine-`
2. Copy `plugins/hermes-kie-evidence/` into
   `~/.hermes/plugins/kor-kie-evidence/`.
3. Configure the plugin settings below in Hermes' `config.yaml`.
4. Enable the plugin with `hermes plugins enable kor-kie-evidence`.
5. Enable the `kor_kie` toolset for the platform/profile that should use it,
   using `hermes tools` or the corresponding toolset configuration.
6. Restart Hermes and verify that `kie_evidence_lookup` is available.

Use the Python interpreter that actually runs Hermes as `HERMES_PYTHON`;
do not assume the system `python` is the same environment. The Evidence Engine
source directory must contain `src/evidence_engine/bridge.py`, and its
database must already exist.

## Operator settings

Example only — replace every path with a real absolute path on the Hermes host:

```yaml
plugins:
  entries:
    kor-kie-evidence:
      settings:
        evidence_python: /absolute/path/to/hermes/python
        evidence_source_root: /absolute/path/to/Evidence-Engine
        evidence_database: /absolute/path/to/evidence.sqlite3
        kie_database: /absolute/path/to/kie-integration.sqlite3
        timeout_seconds: 10
```

All paths are local host settings. They are never accepted as tool arguments.
`evidence_database` must already exist. `kie_database` may be created by KIE
on first use and contains only KIE events and workflow state.

## What happens on a tool call

1. Hermes passes one canonical Evidence task UUID.
2. KIE validates the UUID before opening either database.
3. KIE creates a `hermes_host` request event addressed only to `evidence`.
4. The controlled executor checks the workflow/engine/sensitivity policy.
5. The adapter calls exactly one read-only method: `get_task_report`.
6. KIE persists the minimized output event and workflow transition.
7. Hermes receives only the safe summary, epistemic status, and evidence IDs.

Task titles, success criteria, raw evidence payloads, stdout/stderr, and
verification check details do not enter the KIE event. Evidence Engine remains
the owner of those records.

## Limits and trust boundary

- This is a read-only Evidence integration, not a full two-way Hermes/KIE bus.
- It does not register Critical Thinking, Reliability, or Emotion Engine.
- It does not modify WhatsApp, gateway configuration, the Hermes core, or
  Evidence Engine.
- Installing/enabling the plugin is an explicit operator action.
- The plugin runs as a trusted local Hermes plugin. Filesystem permissions
  still matter; keep the Evidence database and KIE database protected.
- The KIE's app-level policy is not host authentication. The subprocess
  producer label is not cryptographic proof.
- The Evidence Engine's integrity chain is tamper-evident, not tamper-proof.
