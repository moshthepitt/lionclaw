#!/usr/bin/env bash
# Slice 6 real-task eval — the agentic completion gates (need podman + a codex
# auth in ~/.codex). The deterministic gates (moat refusal, advisory-only
# refusal) are covered by `cargo test --test eval_deterministic`.
#
# Usage: scripts/mission-eval.sh [runs]   (default 3 runs per reliability scenario)
# Set LIONCLAW_EVAL_SCOPE=core or methods to run only that acceptance group.
# LIONCLAW_EVAL_METHOD selects one generic method when scope is methods.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/lionclaw"
FIXTURES="$ROOT/crates/lionclaw/tests/fixtures/eval"
RUNS="${1:-3}"
WORK="$(mktemp -d)"
cleanup() {
    if [ "${LIONCLAW_EVAL_KEEP:-0}" = 1 ]; then
        echo "preserved eval work: $WORK" >&2
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

# Install the mission types into an isolated home so the developer's real
# ~/.lionclaw is untouched.
export LIONCLAW_HOME="$WORK/home"
for method in software-dev optimization research review design; do
    "$BIN" install "$ROOT/mission-types/$method" >/dev/null
done

git_quiet() { git -c user.name=eval -c user.email=eval@local -c commit.gpgsign=false "$@"; }

materialize() {
    # materialize <template> <dest>
    rm -rf "$2"; mkdir -p "$2"
    cp -r "$1"/. "$2"/
    ( cd "$2" && cargo generate-lockfile >/dev/null 2>&1 || true )
    git -C "$2" init -q || return
    git_quiet -C "$2" add -A || return
    git_quiet -C "$2" commit -q -m "fixture base" || return
}

mission_json() { "$BIN" mission "$@" --json; }

drive_to_finish() {
    # drive_to_finish <mission-id> <repo>
    local mid="$1" repo="$2" status action
    for _ in $(seq 1 64); do
        status="$(mission_json status "$mid" --repo "$repo")"
        if python3 -c \
            'import json,sys; raise SystemExit(json.load(sys.stdin).get("finish") is None)' \
            <<<"$status"; then
            MISSION_STATUS="$status"
            return 0
        fi
        action="$(python3 -c '
import json, sys
state = json.load(sys.stdin)
choices = state["next"]["choices"]
for choice in choices:
    if choice["kind"] == "continue" and choice["mode"] == "preserve":
        print("continue", choice["effect_id"])
        break
else:
    if any(choice["kind"] == "finish" for choice in choices):
        print("finish")
    elif state["next"]["effects"]:
        print("advance")
    else:
        print("blocked")
' <<<"$status")"
        case "$action" in
            "continue "*)
                "$BIN" mission continue "$mid" "${action#continue }" --repo "$repo" \
                    --reason "eval continues the advertised checkpoint" >/dev/null || return
                ;;
            finish)
                "$BIN" mission finish "$mid" --repo "$repo" \
                    --reason "eval accepts the advertised finish" >/dev/null || return
                ;;
            advance)
                timeout 900 "$BIN" mission advance "$mid" --repo "$repo" --wait >/dev/null || return
                ;;
            *)
                MISSION_STATUS="$status"
                python3 -c '
import json, sys
state = json.load(sys.stdin)
summary = {
    "mission_id": state["mission_id"],
    "choices": state["next"]["choices"],
    "oracle_failures": state["oracle_failures"],
    "failed_judgments": [
        {
            "assertion": assertion["id"],
            "reports": [
                result["receipt"]["handoff"]["content"]
                for result in assertion["advisory_results"]
                if not result["passed"]
            ],
        }
        for assertion in state["contract"]
        if assertion["advisory"] == "failed"
    ],
    "gap_review": state["gap_review"],
}
print("eval stopped on operator-owned choices:", json.dumps(summary), file=sys.stderr)
' <<<"$status"
                return 1
                ;;
        esac
    done
    echo "eval exceeded 64 advertised mission transitions for $mid" >&2
    return 1
}

materialize_method() {
    # materialize_method <fixture> <dest>
    local fixture="$1" repo="$2"
    rm -rf "$repo"
    mkdir -p "$repo"
    case "$fixture" in
        python)
            cat >"$repo/calculator.py" <<'PY'
def add(left, right):
    return left - right
PY
            cat >"$repo/test_calculator.py" <<'PY'
from calculator import add


def test_adds_positive_and_negative_values():
    assert add(7, -2) == 5
PY
            ;;
        javascript)
            cat >"$repo/calculator.js" <<'JS'
function add(left, right) {
    return left - right;
}

module.exports = { add };
JS
            cat >"$repo/calculator.test.js" <<'JS'
const assert = require("node:assert/strict");
const test = require("node:test");

const { add } = require("./calculator");

test("adds positive and negative values", () => {
    assert.equal(add(7, -2), 5);
});
JS
            cat >"$repo/package.json" <<'JSON'
{"name":"lionclaw-js-acceptance","private":true,"scripts":{"test":"node --test"}}
JSON
            ;;
        optimization)
            cat >"$repo/dedupe.py" <<'PY'
def dedupe(values):
    result = []
    probes = 0
    for value in values:
        found = False
        for prior in result:
            probes += 1
            if prior == value:
                found = True
                break
        if not found:
            result.append(value)
    return result, probes
PY
            cat >"$repo/test_dedupe.py" <<'PY'
import unittest
from dedupe import dedupe


class DedupeTest(unittest.TestCase):
    def test_preserves_first_seen_order(self):
        result, _ = dedupe([3, 1, 3, 2, 1])
        self.assertEqual(result, [3, 1, 2])

    def test_preserves_unhashable_value_support(self):
        result, _ = dedupe([[1], [1], [2]])
        self.assertEqual(result, [[1], [2]])

    def test_preserves_non_reflexive_equality_semantics(self):
        value = float("nan")
        result, _ = dedupe([value, value])
        self.assertEqual(len(result), 2)
        self.assertIs(result[0], value)
        self.assertIs(result[1], value)


if __name__ == "__main__":
    unittest.main()
PY
            cat >"$repo/measure.py" <<'PY'
from dedupe import dedupe

values = list(range(5000)) * 2
result, probes = dedupe(values)
assert result == list(range(5000))
assert probes == len(values), f"probe accounting is not one check per input: {probes}"
assert probes < 15000, f"too many equality probes: {probes}"
PY
            cat >"$repo/METRIC.md" <<'EOF'
# Dedupe metric

A probe is one explicit equality predicate or collection-membership predicate
evaluated by `dedupe`. The target is exactly one truthful probe per input
value. `test_dedupe.py`, `measure.py`, and this definition are immutable.
EOF
            ;;
        research)
            mkdir -p "$repo/sources"
            cat >"$repo/sources/release-notes.md" <<'EOF'
# Release notes

Version 2 enables strict validation by default. Published 2026-04-10.
EOF
            cat >"$repo/sources/migration.md" <<'EOF'
# Migration record

The strict default was reverted in version 2.0.1 on 2026-04-12 because legacy
configuration files failed closed. Operators must opt in until version 2.1.
EOF
            ;;
        review)
            cat >"$repo/access.py" <<'PY'
def may_read(requested_user, authenticated_user):
    return bool(authenticated_user)
PY
            cat >"$repo/README.md" <<'EOF'
# Access check

`may_read` must allow a user to read only their own record.
EOF
            ;;
        design)
            cat >"$repo/CONSTRAINTS.md" <<'EOF'
# Export service

- Accept jobs through a local Unix socket.
- Survive process restarts without duplicating a completed export.
- Keep source records read-only.
- Bound retries to three attempts.
- Preserve an operator-visible terminal failure reason.
EOF
            ;;
        *)
            echo "unknown method fixture: $fixture" >&2
            return 1
            ;;
    esac
    git -C "$repo" init -q || return
    git_quiet -C "$repo" add -A || return
    git_quiet -C "$repo" commit -q -m "fixture base" || return
}

run_method_mission() {
    # run_method_mission <type> <fixture> <producer> <judge> <objective>
    #   <task-body> <expected-finish> <head-change>
    local method="$1" fixture="$2" producer="$3" judge="$4"
    local objective="$5" task_body="$6" expected_finish="$7" head_change="$8"
    local repo="$WORK/method-$method" base mid MISSION_STATUS status finish head
    materialize_method "$fixture" "$repo" || return
    base="$(git -C "$repo" rev-parse HEAD)"
    mid="$(mission_json start --type "$method" --repo "$repo" --objective "$objective" \
        | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')" || return
    mission_json team show --mission-id "$mid" --repo "$repo" >"$repo/team.json" || return
    python3 - "$repo/team.json" "$repo/plan.json" "$method" "$fixture" "$producer" "$judge" \
        "$task_body" <<'PY'
import json, sys

team = json.load(open(sys.argv[1]))["team"]
method, fixture, producer, judge, task_body = sys.argv[3:]
team["revision"] = 1
team["task_assignments"] = {
    "work": {"type": "role", "role_instance": producer}
}

assertions = [{
    "id": "METHOD-RESULT",
    "prose": "the method result satisfies the objective without overstating evidence",
    "oracle": None,
}]
requirements = [{
    "id": "METHOD-OUTCOME",
    "kind": "capability",
    "prose": "the requested method outcome is complete and evidence-grounded",
    "disposition": {
        "type": "reviewer_checkable",
        "assertion_ids": ["METHOD-RESULT"],
    },
}]
oracles = {}

if method == "software-dev" and fixture == "python":
    assertions = [{
        "id": "PYTEST-PASSES",
        "prose": "pytest passes at the final commit",
        "oracle": "python-tests",
    }, {
        "id": "RUFF-PASSES",
        "prose": "ruff passes at the final commit",
        "oracle": "python-ruff",
    }]
    requirements[0]["disposition"]["type"] = "confined_provable"
    requirements[0]["disposition"]["assertion_ids"] = [
        "PYTEST-PASSES",
        "RUFF-PASSES",
    ]
    oracles["python-tests"] = {
        "type": "command",
        "argv": ["python3", "-m", "pytest", "-q"],
        "cwd": ".",
        "timeout_secs": 300,
    }
    oracles["python-ruff"] = {
        "type": "command",
        "argv": ["ruff", "check", "--no-cache", "."],
        "cwd": ".",
        "timeout_secs": 300,
    }
elif method == "software-dev" and fixture == "javascript":
    assertions[0] = {
        "id": "PACKAGE-TESTS",
        "prose": "the package-manager test command passes at the final commit",
        "oracle": "package-tests",
    }
    requirements[0]["disposition"]["type"] = "confined_provable"
    requirements[0]["disposition"]["assertion_ids"] = ["PACKAGE-TESTS"]
    oracles["package-tests"] = {
        "type": "command",
        "argv": ["npm", "test", "--silent"],
        "cwd": ".",
        "timeout_secs": 300,
    }
elif method == "optimization":
    assertions.insert(0, {
        "id": "MEASURE-PASSES",
        "prose": "the repository metric check passes at the final commit",
        "oracle": "python-measure",
    })
    requirements.insert(0, {
        "id": "MEASURED-IMPROVEMENT",
        "kind": "capability",
        "prose": "the equality-probe count is below the repository threshold",
        "disposition": {
            "type": "confined_provable",
            "assertion_ids": ["MEASURE-PASSES"],
        },
    })
    oracles["python-measure"] = {
        "type": "command",
        "argv": ["python3", "measure.py"],
        "cwd": ".",
        "timeout_secs": 300,
    }
elif method == "review":
    assertions[0]["prose"] = (
        "the report identifies that access.py never compares user identities, "
        "demonstrates a truthy cross-user read, and classifies the resulting "
        "authorization bypass without claiming unsupported impact"
    )

team["judgment_assignments"] = {
    assertion["id"]: [judge] for assertion in assertions
}
proposal = {
    "plan": {
        "base_revision": 0,
        "requirement_changes": [],
        "assertion_supersessions": [],
        "plan": {
            "requirements": requirements,
            "assertions": assertions,
            "tasks": [{
                "id": "work",
                "body": task_body,
                "targets": [assertion["id"] for assertion in assertions],
                "depends_on": [],
            }],
        },
    },
    "team": team,
    "oracles": oracles,
}
json.dump(proposal, open(sys.argv[2], "w"))
PY
    [ "$?" -eq 0 ] || return
    "$BIN" mission plan propose "$mid" --repo "$repo" --file "$repo/plan.json" >/dev/null ||
        return
    "$BIN" mission decide "$mid" plan_proposal:mission approve --repo "$repo" \
        --justification "eval approves the repository-specific method plan" >/dev/null || return
    if ! drive_to_finish "$mid" "$repo"; then
        echo "  $method/$fixture: FAIL (mission did not reach an advertised finish)"
        return 1
    fi
    status="$MISSION_STATUS"
    finish="$(python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))' \
        <<<"$status")"
    head="$(python3 -c 'import sys,json;print(json.load(sys.stdin).get("current_sha"))' \
        <<<"$status")"
    if [ "$finish" != "$expected_finish" ]; then
        echo "  $method/$fixture: FAIL (finish=$finish, expected=$expected_finish)"
        return 1
    fi
    if { [ "$head_change" = changed ] && [ "$head" = "$base" ]; } ||
        { [ "$head_change" = unchanged ] && [ "$head" != "$base" ]; }; then
        echo "  $method/$fixture: FAIL (head=$head, base=$base, expected=$head_change)"
        return 1
    fi
    echo "  $method/$fixture: PASS ($finish, head $head_change)"
}

# --- Scenario 1: fixes the interval-bug and reaches a verified finish -------
scenario_fix_bug() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/interval-$i"
        materialize "$FIXTURES/interval-bug" "$repo" || return
        local base; base="$(git -C "$repo" rev-parse HEAD)"
        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Fix the off-by-one in overlaps() so all tests pass. Do not weaken any test." \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')" || return
        mission_json team show --mission-id "$mid" --repo "$repo" > "$repo/team.json" || return
        python3 - "$repo/team.json" "$repo/plan.json" <<'PY'
import json, sys
team = json.load(open(sys.argv[1]))["team"]
team["revision"] = 1
team["task_assignments"] = {
    "fix": {"type": "role", "role_instance": "implementer"}
}
team["judgment_assignments"] = {"TESTS-PASS": ["reviewer"]}
proposal = {
    "plan": {
        "base_revision": 0,
        "requirement_changes": [],
        "assertion_supersessions": [],
        "plan": {
            "requirements": [{
                "id": "CORRECT-OVERLAPS",
                "kind": "capability",
                "prose": "closed intervals that touch overlap",
                "disposition": {
                    "type": "confined_provable",
                    "assertion_ids": ["TESTS-PASS"],
                },
            }],
            "assertions": [{
                "id": "TESTS-PASS",
                "prose": "cargo test passes at the final commit",
                "oracle": "cargo-test",
            }],
            "tasks": [{
                "id": "fix",
                "body": "Fix overlaps() for closed intervals. Do not modify the tests.",
                "targets": ["TESTS-PASS"],
                "depends_on": [],
            }],
        },
    },
    "team": team,
    "oracles": {
        "cargo-test": {
            "type": "command",
            "argv": ["cargo", "test", "--locked"],
            "cwd": ".",
            "environment": {
                "CARGO_HOME": "/scratch/cargo",
                "CARGO_TARGET_DIR": "/scratch/target",
            },
            "timeout_secs": 900,
        },
    },
}
json.dump(proposal, open(sys.argv[2], "w"))
PY
        [ "$?" -eq 0 ] || return
        "$BIN" mission plan propose "$mid" --repo "$repo" --file "$repo/plan.json" >/dev/null ||
            return
        "$BIN" mission decide "$mid" plan_proposal:mission approve --repo "$repo" \
            --justification "eval approves the fixture plan" >/dev/null || return
        local MISSION_STATUS
        if ! drive_to_finish "$mid" "$repo"; then
            echo "  run $i: FAIL (mission did not reach an advertised finish)"
            continue
        fi
        local status="$MISSION_STATUS"
        local finish head; finish="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))')"
        head="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("current_sha"))')"
        if [ "$finish" = "verified" ] && [ "$head" != "$base" ]; then
            echo "  run $i: PASS (verified, head $head != base)"; pass=$((pass+1))
        else
            echo "  run $i: FAIL (finish=$finish, head=$head, base=$base)"
        fi
    done
    echo "scenario 1 (fix bug): $pass/$RUNS verified"
    if [ "$pass" -ge $(((RUNS + 1) / 2)) ]; then
        echo "  => SCENARIO 1 PASS"
    else
        echo "  => SCENARIO 1 FAIL"
        return 1
    fi
}

# --- Scenario 2: planning-in-phase -> approve -> verified -----------------
# No hand-written plan: the team's planner proposes the contract and
# complete assignments, a human approves them, then execution verifies. The
# Plan approval is always explicit, so planning must park before any work.
scenario_planning() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/planning-$i"
        materialize "$FIXTURES/interval-bug" "$repo" || return
        local base; base="$(git -C "$repo" rev-parse HEAD)"
        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Fix the off-by-one in overlaps() so all tests pass. Do not weaken any test." \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')" || return
        # Drive planning; it must park on the engine-authored proposal.
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" --wait >/dev/null || return
        # Approve the proposal (seeds the contract), then execute to a verdict.
        "$BIN" mission decide "$mid" plan_proposal:mission approve --repo "$repo" \
            --justification "eval approves the generated plan" >/dev/null || return
        local MISSION_STATUS
        if ! drive_to_finish "$mid" "$repo"; then
            echo "  run $i: FAIL (mission did not reach an advertised finish)"
            continue
        fi
        local status="$MISSION_STATUS"
        local finish head
        finish="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))')"
        head="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("current_sha"))')"
        if [ "$finish" = "verified" ] && [ "$head" != "$base" ]; then
            echo "  run $i: PASS (planned, approved, verified; head $head)"; pass=$((pass+1))
        else
            echo "  run $i: FAIL (finish=$finish, head=$head, base=$base)"
        fi
    done
    echo "scenario 2 (planning -> approve -> verified): $pass/$RUNS verified"
    if [ "$pass" -ge $(((RUNS + 1) / 2)) ]; then
        echo "  => SCENARIO 2 PASS"
    else
        echo "  => SCENARIO 2 FAIL"
        return 1
    fi
}

scenario_methods() {
    local failures=0
    local selected="${LIONCLAW_EVAL_METHOD:-all}"
    case "$selected" in
        all | software-dev | optimization | research | review | design) ;;
        *)
            echo "LIONCLAW_EVAL_METHOD names an unknown method: $selected" >&2
            return 2
            ;;
    esac
    if [ "$selected" = all ] || [ "$selected" = software-dev ]; then
        run_method_mission \
        software-dev python implementer reviewer \
        "Fix calculator.add so pytest and ruff pass. Do not change the tests." \
        "Correct calculator.add, preserve the tests, and commit the fix." \
        verified changed || failures=1
        run_method_mission \
        software-dev javascript implementer reviewer \
        "Fix calculator.add so the package-manager test passes. Do not change the tests or package.json." \
        "Correct calculator.add, preserve the tests and package.json, and commit the fix." \
        verified changed || failures=1
    fi
    if [ "$selected" = all ] || [ "$selected" = optimization ]; then
        run_method_mission \
        optimization optimization optimizer reviewer \
        "Reduce dedupe probes to one per input on the measured workload while preserving first-seen order, unhashable inputs, non-reflexive equality semantics, and the metric definition. Do not change tests, measure.py, or METRIC.md." \
        "Implement and commit a coherent dedupe optimization with truthful probe accounting and no input-domain or equality regression. Preserve test_dedupe.py, measure.py, and METRIC.md." \
        attested changed || failures=1
    fi
    if [ "$selected" = all ] || [ "$selected" = research ]; then
        run_method_mission \
        research research researcher reviewer \
        "Determine the currently effective strict-validation default from the local sources, preserving chronology and uncertainty in REPORT.md." \
        "Create and commit REPORT.md with a source-cited conclusion grounded only in sources/." \
        attested changed || failures=1
    fi
    if [ "$selected" = all ] || [ "$selected" = review ]; then
        run_method_mission \
        review review investigator reviewer \
        "Audit access.py against the ownership rule in README.md and report concrete findings without modifying the product." \
        "Review access.py read-only and return a typed report with evidence and severity." \
        attested unchanged || failures=1
    fi
    if [ "$selected" = all ] || [ "$selected" = design ]; then
        run_method_mission \
        design design designer reviewer \
        "Design the constrained export service described in CONSTRAINTS.md and record a decision-ready DESIGN.md." \
        "Create and commit DESIGN.md covering state, idempotency, recovery, failure reporting, and alternatives." \
        attested changed || failures=1
    fi
    return "$failures"
}

failures=0
case "${LIONCLAW_EVAL_SCOPE:-all}" in
    all | core)
        echo "== Agentic reliability eval ($RUNS runs each) =="
        echo "[1] fixes the bug -> verified"
        scenario_fix_bug || failures=1
        echo "[2] planning -> approve -> verified"
        scenario_planning || failures=1
        if [ "${LIONCLAW_EVAL_SCOPE:-all}" = core ]; then
            exit "$failures"
        fi
        ;;
    methods)
        ;;
    *)
        echo "LIONCLAW_EVAL_SCOPE must be all, core, or methods" >&2
        exit 2
        ;;
esac
echo "== Generic method acceptance =="
scenario_methods || failures=1
exit "$failures"
