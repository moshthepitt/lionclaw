# Metric-Driven Missions

Use this mission type when the objective is to improve or preserve a measured
score rather than ship a software feature. The plan should name the metric,
state the acceptance threshold or comparison baseline, assign exactly scoped
experiment work, and bind deterministic measurement assertions to
mission-local command oracles. Each oracle declaration is part of the proposal:
a structured executable and argument vector, clean workspace-relative
directory, bounded environment, timeout, authority, and resources. Use the
project's reproducible benchmark or evaluation entrypoint. Never invoke a
shell, and never treat an agent-authored result file as independent proof of
the result it claims.

Where the metric depends on subjective quality or external interpretation,
classify that requirement as reviewer-checkable or host-acceptance instead of
pretending it is worker-independent proof.

Metric objectives must not require the gap reviewer to prove that a finish was
attempted. The gap review runs before finish is legal, so finish-attempt
evidence is structurally unavailable at review time.

When experiments need dependency installs or large temporary working space,
use workspace-local or scratch-local prefixes and explicit resource overrides
within the mission ceilings. Proof commands run over the judged tree without
secrets, network, installs, or writes. Device passthrough is optional and must
be requested explicitly within the device ceiling; absence of a grant means no
device flag.
