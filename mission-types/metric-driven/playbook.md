# Metric-Driven Missions

Use this mission type when the objective is to improve or preserve a measured
score rather than ship a software feature. The plan should name the metric,
state the acceptance threshold or comparison baseline, assign exactly scoped
experiment work, and bind deterministic measurement assertions to the metric
oracle only when the workspace contains the oracle's pinned condition files:
`metric.txt` for the measured scalar, `metric.expected` for the threshold or
baseline, and `metric.operator` with `gte`, `lte`, or `eq`.

Where the metric depends on subjective quality or external interpretation,
classify that requirement as reviewer-checkable or host-acceptance instead of
pretending it is worker-independent proof.

Metric objectives must not require the gap reviewer to prove that a finish was
attempted. The gap review runs before finish is legal, so finish-attempt
evidence is structurally unavailable at review time.

When experiments need dependency installs or large temporary working space, use
workspace-local or scratch-local prefixes and explicit resource overrides
within the mission ceilings. Device passthrough is optional and must be
requested explicitly by a role or oracle declaration within the device ceiling;
absence of a grant means no device flag.
