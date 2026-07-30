#!/usr/bin/env bash
# Slice 6 real-task eval — the agentic completion gates (need podman + a codex
# auth in ~/.codex). The deterministic gates (moat refusal, advisory-only
# refusal) are covered by `cargo test --test eval_deterministic`.
#
# Usage: scripts/mission-eval.sh [runs]   (default 3 runs per agentic scenario)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/lionclaw"
FIXTURES="$ROOT/crates/lionclaw/tests/fixtures/eval"
RUNS="${1:-3}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Install the mission types into an isolated home so `--type software-dev`
# resolves and the dev's real ~/.lionclaw is untouched.
export LIONCLAW_HOME="$WORK/home"
"$BIN" install "$ROOT/mission-types/software-dev" >/dev/null

git_quiet() { git -c user.name=eval -c user.email=eval@local -c commit.gpgsign=false "$@"; }

materialize() {
    # materialize <template> <dest>
    rm -rf "$2"; mkdir -p "$2"
    cp -r "$1"/. "$2"/
    ( cd "$2" && cargo generate-lockfile >/dev/null 2>&1 || true )
    git -C "$2" init -q
    git_quiet -C "$2" add -A
    git_quiet -C "$2" commit -q -m "fixture base"
}

mission_json() { "$BIN" mission "$@" --json; }

# --- Scenario 1: fixes the interval-bug and reaches a verified finish -------
scenario_fix_bug() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/interval-$i"
        materialize "$FIXTURES/interval-bug" "$repo"
        local base; base="$(git -C "$repo" rev-parse HEAD)"
        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Fix the off-by-one in overlaps() so all tests pass. Do not weaken any test." \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')"
        mission_json team show --mission-id "$mid" --repo "$repo" > "$repo/team.json"
        python3 - "$repo/team.json" "$repo/plan.json" <<'PY'
import json, sys
team = json.load(open(sys.argv[1]))["team"]
team["revision"] = 1
team["task_assignments"] = {"fix": "implementer"}
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
        "$BIN" mission plan propose "$mid" --repo "$repo" --file "$repo/plan.json" >/dev/null
        "$BIN" mission decide "$mid" plan_proposal:mission approve --repo "$repo" \
            --justification "eval approves the fixture plan" >/dev/null
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" --wait >/dev/null
        local status; status="$(mission_json status "$mid" --repo "$repo")"
        local finish head; finish="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))')"
        head="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("current_sha"))')"
        if [ "$finish" = "verified" ] && [ "$head" != "$base" ]; then
            echo "  run $i: PASS (verified, head $head != base)"; pass=$((pass+1))
        else
            echo "  run $i: FAIL (finish=$finish, head=$head, base=$base)"
        fi
    done
    echo "scenario 1 (fix bug): $pass/$RUNS verified"
    if [ "$pass" -ge 2 ]; then
        echo "  => SCENARIO 1 PASS"
    else
        echo "  => SCENARIO 1 FAIL"
        return 1
    fi
}

# NOTE: the former "reviewer catches a planted regression" scenario was removed.
# It needs a reviewer-only (oracle-less) assertion, which is only valid
# under a `stop = attested` mission type — and no product mission type ships with
# that bar today (software-dev is `verified`, which rejects an oracle-less
# assertion when proposed). The advisory-only-can't-verify behavior it checked stays
# covered deterministically by `cargo test --test eval_deterministic`. Restore an
# agentic reviewer scenario once an `attested`-stop mission type (e.g. code-review)
# ships.

# --- Scenario 2: planning-in-phase -> approve -> verified -----------------
# No hand-written plan: the team's strategist proposes the contract and
# complete assignments, a human approves them, then execution verifies. The
# Plan approval is always explicit, so planning must park before any work.
scenario_planning() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/planning-$i"
        materialize "$FIXTURES/interval-bug" "$repo"
        local base; base="$(git -C "$repo" rev-parse HEAD)"
        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Fix the off-by-one in overlaps() so all tests pass. Do not weaken any test." \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')"
        # Drive planning; it must park on the engine-authored proposal.
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" --wait >/dev/null
        # Approve the proposal (seeds the contract), then execute to a verdict.
        "$BIN" mission decide "$mid" plan_proposal:mission approve --repo "$repo" \
            --justification "eval approves the generated plan" >/dev/null
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" --wait >/dev/null
        local status; status="$(mission_json status "$mid" --repo "$repo")"
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
    if [ "$pass" -ge 2 ]; then
        echo "  => SCENARIO 2 PASS"
    else
        echo "  => SCENARIO 2 FAIL"
        return 1
    fi
}

failures=0
echo "== Slice 6 agentic eval ($RUNS runs each) =="
echo "[1] fixes the bug -> verified"
scenario_fix_bug || failures=1
echo "[2] planning -> approve -> verified"
scenario_planning || failures=1
exit "$failures"
