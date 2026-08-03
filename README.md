# LionClaw

LionClaw runs real agents under a small trusted core and explicit local
boundary.

Supported platform: Linux x86_64.

Download the latest `lionclaw-*-linux-x86_64.tar.gz` and `SHA256SUMS` from the
[GitHub releases](https://github.com/moshthepitt/lionclaw/releases), verify the
archive with `sha256sum -c`, then install the extracted `lionclaw/` directory
through your agent harness's normal skill installation mechanism.

```text
lionclaw doctor
```

```text
lionclaw run codex
```

---

For other runtimes, see `lionclaw run --help`. For mission operation, see
`lionclaw mission --help`. To author custom mission types, see
`lionclaw mission type --help`.
