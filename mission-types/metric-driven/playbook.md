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
