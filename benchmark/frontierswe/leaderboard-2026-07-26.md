# FrontierSWE Leaderboard and Scoring Snapshot

Snapshot time: 2026-07-26 (Africa/Nairobi).

The public Mean@5 leaderboard at snapshot time was:

| Rank | Model | Harness | Avg rank | Dominance |
| ---: | --- | --- | ---: | ---: |
| 1 | Claude Fable 5 | Claude Code | 2.47 | 89% |
| 2 | Grok 4.5 | Grok CLI | 4.09 | 78% |
| 3 | Claude Opus 4.8 | Claude Code | 4.82 | 73% |
| 4 | GLM-5.2 | Claude Code | 4.85 | 72% |
| 5 | GPT-5.5 | Codex | 5.21 | 70% |
| 6 | Claude Opus 4.7 | Claude Code | 6.47 | 61% |
| 7 | Claude Opus 4.6 | Claude Code | 7.59 | 53% |
| 8 | GPT-5.4 | Codex | 7.88 | 51% |
| 9 | Composer 2.5 | Cursor CLI | 9.65 | 38% |
| 10 | Gemini 3.1 Pro | Gemini CLI | 9.79 | 37% |
| 11 | GLM-5.1 | Claude Code | 11.00 | 29% |
| 12 | DeepSeek V4 Pro | Claude Code | 11.18 | 27% |
| 13 | Kimi K2.6 | Kimi CLI | 11.44 | 25% |
| 14 | Kimi K2.5 | Kimi CLI | 11.50 | 25% |
| 15 | Qwen3.6-Plus | Qwen Code | 12.06 | 21% |

The headline Zenith comparison is a separate Intelligent Internet result, not
a row on the public table above at snapshot time. Its 2026-06-29 report claims
Mean@5 average rank 2.06 and 92% dominance for GPT-5.5 with Zenith, versus
average rank 5.53 for its GPT-5.5/Codex comparison snapshot. The moving public
table now reports GPT-5.5/Codex at 5.21, which is why both states are preserved
rather than silently substituting the newer number into the original claim.

## Exact scoring rules

The pinned `SCORING.md` names `scripts/score_from_reward.py` as the source of
truth. Each task's rich `reward.json` is transformed as follows:

- implementation: correctness;
- performance: `0.5 * correctness` until correctness is exactly 1, then
  `0.5 + 0.5 * speedup`;
- ML research: raw reward, except `frogsgame-rl` divides solved-board count by
  500;
- `notebook-compression`: speedup only when fully correct, otherwise zero;
- `libexpat-to-x86asm`: the performance gate with uncapped speedup.

For `dependent-type-checker`, partial correctness is
`(accept_passed + reject_passed) / (accept_total + reject_total)`. If its
correctness gate passed and raw reward is positive, correctness becomes 1 and
raw reward is the throughput speedup. The normal performance gate then yields
the leaderboard score.

Avg and Best are the mean and maximum gated scores across five trials.
Correctness X/5 counts trials with exactly 100% correctness; it is not the
partial score. Any trial flagged by the separate post-hoc anti-cheat audit is
zeroed. Global average rank is the mean task position (lower is better);
dominance is the task-wise win probability against a randomly selected
opponent.
