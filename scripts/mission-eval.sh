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
"$BIN" install --from "$ROOT/mission-types" >/dev/null

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
{ "assertions": [ { "id": "TESTS-PASS", "prose": "cargo test passes at the final commit", "oracle": "cargo-test" } ],
  "tasks": [ { "id": "fix", "kind": "work", "body": "Fix overlaps() for closed intervals so tests::touching_intervals_overlap and merge_coalesces_touching_intervals pass. Do not modify the tests.", "targets": ["TESTS-PASS"], "role": "implementer", "depends_on": [] } ] }
JSON
        "$BIN" mission submit-plan "$mid" --repo "$repo" --plan "$repo/plan.json" >/dev/null 2>&1
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

# --- Scenario 2: reviewer catches a planted regression ---------------------
# The harness commits a patch that keeps `cargo test` GREEN but breaks the
# documented capacity invariant, then dispatches the reviewer against that
# commit. The reviewer must return passed:false for the invariant assertion.
scenario_planted_regression() {
    local pass=0
    for i in $(seq 1 "$RUNS"); do
        local repo="$WORK/cache-$i"
        materialize "$FIXTURES/cache-invariant" "$repo"
        # Plant the regression: remove the eviction so capacity can be
        # exceeded. Existing tests never overflow, so they stay green.
        python3 - "$repo/src/lib.rs" <<'PY'
import sys, re
p = sys.argv[1]
s = open(p).read()
s = s.replace(
    "        if self.values.len() >= self.capacity {\n            let oldest = self.order.remove(0);\n            self.values.retain(|(k, _)| k != &oldest);\n        }\n",
    "        // regression: eviction removed; capacity can now be exceeded\n")
open(p, "w").write(s)
PY
        git_quiet -C "$repo" commit -aqm "perf: drop eviction on insert"
        local regressed; regressed="$(git -C "$repo" rev-parse HEAD)"

        local mid; mid="$(mission_json start --type software-dev --repo "$repo" \
            --objective "Review the cache for correctness against its documented invariant." --yes \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["mission_id"])')"
        # A no-op work task keeps the (regressed) commit as the judged state,
        # then the reviewer judges it.
        cat > "$repo/plan.json" <<JSON
{ "assertions": [ { "id": "CAP-INVARIANT", "prose": "BoundedCache.len() never exceeds capacity(); insertion past capacity must evict the oldest entry" } ],
  "tasks": [ { "id": "keep", "kind": "work", "body": "Do not change any code. Add a one-line comment to src/lib.rs acknowledging the review is pending, then commit.", "targets": ["CAP-INVARIANT"], "role": "implementer", "depends_on": [] },
             { "id": "review", "kind": "validate", "body": "Judge whether BoundedCache preserves its documented capacity invariant. Read put(); if inserting past capacity no longer evicts, the invariant is VIOLATED and you must report passed:false for CAP-INVARIANT.", "targets": ["CAP-INVARIANT"], "role": "reviewer", "depends_on": ["keep"] } ] }
JSON
        "$BIN" mission submit-plan "$mid" --repo "$repo" --plan "$repo/plan.json" >/dev/null 2>&1
        timeout 900 "$BIN" mission advance "$mid" --repo "$repo" >/dev/null 2>&1
        local status; status="$(mission_json status "$mid" --repo "$repo")"
        # The reviewer must have marked CAP-INVARIANT advisory=failed, AND the
        # mission must NOT be verified (advisory alone can't verify).
        local advisory finish; advisory="$(echo "$status" | python3 -c '
import sys,json
d=json.load(sys.stdin)
c=[a for a in d["contract"] if a["id"]=="CAP-INVARIANT"]
print(c[0]["advisory"] if c else "missing")')"
        finish="$(echo "$status" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("finish"))')"
        if [ "$advisory" = "failed" ] && [ "$finish" != "verified" ]; then
            echo "  run $i: PASS (reviewer caught it: advisory=failed, finish=$finish)"; pass=$((pass+1))
        else
            echo "  run $i: FAIL (advisory=$advisory, finish=$finish, regressed=$regressed)"
        fi
    done
    echo "scenario 2 (planted regression): $pass/$RUNS caught"
    [ "$pass" -ge 2 ] && echo "  => SCENARIO 2 PASS" || echo "  => SCENARIO 2 FAIL"
}

echo "== Slice 6 agentic eval ($RUNS runs each) =="
echo "[1] fixes the bug -> verified"
scenario_fix_bug
echo "[2] reviewer catches a planted regression"
scenario_planted_regression
