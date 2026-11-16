# KOR Lilith Engine Stack Installation

The KOR stack is distributed as one native Hermes plugin pack. It installs five
separate plugins pinned to immutable Git commits:

1. **Evidence Engine** — coding-only verification gate and evidence ledger
2. **Critical Thinking Engine** — review routing and critical checks
3. **Reliability Engine** — tool/runtime safeguards and operational observations
4. **Emotion Engine** — bounded self-observation and reflection
5. **KOR Integration Engine (KIE)** — coordination, event ledger, and read-only status tools

KIE is the coordinator, not a sixth domain engine. The other four remain separate
plugins with their own code and responsibilities.

## Install the complete stack in one operation

Hermes Agent v0.23.1 includes native plugin-pack support. From a terminal, use
the hosted pack directly:

```bash
hermes plugins pack show https://raw.githubusercontent.com/oORaxaOo/KOR-Integration-Engine-/main/hermes-pack.yaml
hermes plugins pack install https://raw.githubusercontent.com/oORaxaOo/KOR-Integration-Engine-/main/hermes-pack.yaml
```

The first command is a dry run. The second displays all five sources, commit pins,
and declared capabilities, then asks for one confirmation before installing the
pack. Hermes installs each entry through its normal pinned-plugin installer and
enables successful installs. A failed entry is reported separately; the other
entries may still install, and the pack command exits non-zero when any entry
fails. Do not treat a partial installation as a healthy stack.

The pack contains no secrets and no machine-specific paths. It seeds the same
private KIE socket path and standard home-directory database/source paths for all
five plugins. Hermes preserves values the operator has already configured; the
pack only fills missing settings. Hermes retains its normal security scanning and
per-plugin capability consent. It does not bulk-grant privileges. The pack is
declarative: KIE does not clone or install repositories when it loads.

## Important Evidence Engine behavior

Evidence Engine is included as its own plugin. Its coding verification gate runs
only for coding turns with changed files and an operator-approved project policy.
It fails closed when required verification cannot be completed.

Its `transform_llm_output` hook uses `hold_stream=True`. While the plugin is
enabled, Hermes holds final-response streaming until the final response is
available; non-coding replies pass through unchanged, but they also wait for the
final-response stage. This is an intentional safety trade-off, not an invisible
zero-cost hook. Review this behavior before enabling the pack in the profile used
for Lilith.

Evidence Engine does not expose database writers or arbitrary command execution
as model-callable tools. Its project commands must be fixed in an operator-owned
policy file. The policy is a trust boundary, not an OS sandbox.

The pack supplies standard non-secret defaults. It does not create or pre-authorize
an Evidence project policy.

## Post-install configuration

The pack seeds the standard paths. The active Hermes Python interpreter is
discovered by KIE at runtime. Confirm the seeded settings in the existing Hermes
profile rather than replacing unrelated settings.

### Evidence Engine

Evidence Engine defaults to:

- Database: `~/.hermes/evidence-engine/evidence.sqlite3`
- Project policy: `~/.hermes/evidence-engine/policies.json`
- Maximum attempts: 5

Create the private policy directory and start from the repository example:

```bash
mkdir -p ~/.hermes/evidence-engine
chmod 700 ~/.hermes/evidence-engine
cp ~/.hermes/plugins/evidence-engine/examples/hermes-policies.example.json ~/.hermes/evidence-engine/policies.json
chmod 600 ~/.hermes/evidence-engine/policies.json
```

Edit the policy to the exact workspace roots and fixed test/build commands that
you approve. Until a valid policy exists for a workspace, coding verification
will fail closed for that workspace; this is not an engine crash.

### KIE and the shared engine connection

The pack seeds these matching values for KIE, Evidence, Critical Thinking,
Reliability, and Emotion:

- Event socket: `~/.hermes/run/kie-engine-events.sock`
- KIE ledger/workflow database: `~/.hermes/kie/kie.sqlite3`
- Evidence source checkout: `~/.hermes/plugins/evidence-engine`
- Evidence database: `~/.hermes/evidence-engine/evidence.sqlite3`

KIE uses the active Hermes Python interpreter automatically. The Evidence source
path must contain `src/evidence_engine/bridge.py`. If you use a non-default
`HERMES_HOME`, a custom plugin directory, or a custom Evidence database, update
the matching paths in KIE settings so both plugins address the same install and
database.

The socket is private local IPC, not a network listener. Keep it inside the
Hermes user's private directory; do not place it in a shared directory.

### Restart and verify

After saving the profile configuration, restart the Hermes process and inspect:

```bash
hermes plugins
hermes plugins doctor evidence-engine
hermes plugins doctor critical-thinking-engine
hermes plugins doctor reliability-engine
hermes plugins doctor emotion-engine
hermes plugins doctor kor-kie-evidence
```

After restart, ask Lilith for a KIE system health report or call the read-only
`kie_system_health` tool. KIE also checks health before normal LLM turns and
injects a deduplicated system-health warning when a component is missing or an
allowlisted failure was observed. The alert is delivered on the next normal turn;
KIE does not make an extra LLM call or continuously poll idle engines.

The health report covers all four domain engines, event-socket availability,
Evidence bridge configuration, and recent allowlisted Reliability/CTE/Emotion/
Evidence failure events. Old failure events expire from the active alert window.
The system does not automatically restart plugins or change their code.

The pack install result proves only that repository checkouts succeeded. The
doctor checks and KIE runtime health report are the next layer of verification.
A complete live integration test must confirm that KIE receives at least one
valid event from each supporting engine and can successfully read the configured
Evidence task database.

## Version updates

Update the five commit pins together only after the repositories pass their own
CI and the cross-repository compatibility check. This pack defines one tested
stack; it does not follow moving branches or silently install the newest commits.
