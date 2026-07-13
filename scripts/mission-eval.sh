#!/usr/bin/env bash
# Slice 6 real-task eval — the agentic completion gates (need podman + a codex
# auth in ~/.codex). The deterministic gates (moat refusal, advisory-only
# refusal) are covered by `cargo test --test eval_deterministic`.
#
# Usage: scripts/mission-eval.sh [runs]   (default 3 runs per agentic scenario)
set -uo pipefail

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

mission_json() { "$BIN" mission "$@" --json 2>/dev/null; }

# --- Scenario 1: fixes the interval-bug and reaches a verified finish -------
scenario_fix_bug() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/interval-$i"
        materialize "$FIXTURES/interval-bug" "$repo"
        local base; base="$(git -C "$repo" rev-parse HEAD)"
        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Fix the off-by-one in overlaps() so all tests pass. Do not weaken any test." \
            --yes | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')"
        cat > "$repo/plan.json" <<JSON
{ "base_revision": 0, "plan": {
  "requirements": [ { "id": "CORRECT-OVERLAPS", "kind": "capability", "prose": "closed intervals that touch overlap", "disposition": { "type": "covered", "assertion_ids": ["TESTS-PASS"] } } ],
  "assertions": [ { "id": "TESTS-PASS", "prose": "cargo test passes at the final commit", "oracle": "cargo-test" } ],
  "tasks": [ { "id": "fix", "kind": "work", "body": "Fix overlaps() for closed intervals so tests::touching_intervals_overlap and merge_coalesces_touching_intervals pass. Do not modify the tests.", "targets": ["TESTS-PASS"], "role": "implementer", "depends_on": [] } ] } }
JSON
        "$BIN" mission plan propose "$mid" --repo "$repo" --file "$repo/plan.json" >/dev/null 2>&1
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" >/dev/null 2>&1
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
    [ "$pass" -ge 2 ] && echo "  => SCENARIO 1 PASS" || echo "  => SCENARIO 1 FAIL"
}

# NOTE: the former "reviewer catches a planted regression" scenario was removed.
# It needs a reviewer-only (oracle-less) assertion, which is only valid
# under a `stop = reviewed` mission type — and no product mission type ships with
# that bar today (software-dev is `verified`, which rejects an oracle-less
# assertion when proposed). The advisory-only-can't-verify behavior it checked stays
# covered deterministically by `cargo test --test eval_deterministic`. Restore an
# agentic reviewer scenario once a `reviewed`-stop mission type (e.g. code-review)
# ships.

# --- Scenario 2: planning-in-phase -> approve -> verified -----------------
# No hand-written plan: the planning DAG (strategist -> red-team -> author)
# proposes the contract, a human approves it, then execution verifies. The
# approval gate is ON (no --yes), so planning must park before any work.
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
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" >/dev/null 2>&1
        local phase; phase="$(mission_json status "$mid" --repo "$repo" \
            | python3 -c 'import sys,json;print(json.load(sys.stdin).get("phase"))')"
        # Approve the proposal (seeds the contract), then execute to a verdict.
        local item; item="$(mission_json status "$mid" --repo "$repo" \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["attention"][0]["id"])')"
        "$BIN" mission decide "$mid" "$item" approve --repo "$repo" >/dev/null 2>&1
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" >/dev/null 2>&1
        local status; status="$(mission_json status "$mid" --repo "$repo")"
        local finish head
        finish="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))')"
        head="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("current_sha"))')"
        if [ "$finish" = "verified" ] && [ "$head" != "$base" ]; then
            echo "  run $i: PASS (planned, approved, verified; head $head)"; pass=$((pass+1))
        else
            echo "  run $i: FAIL (phase-after-plan=$phase, finish=$finish, head=$head, base=$base)"
        fi
    done
    echo "scenario 2 (planning -> approve -> verified): $pass/$RUNS verified"
    [ "$pass" -ge 2 ] && echo "  => SCENARIO 2 PASS" || echo "  => SCENARIO 2 FAIL"
}

echo "== Slice 6 agentic eval ($RUNS runs each) =="
echo "[1] fixes the bug -> verified"
scenario_fix_bug
echo "[2] planning -> approve -> verified"
scenario_planning
