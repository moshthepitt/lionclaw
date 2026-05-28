+++
title = "The run command became the console"
date = 2026-05-25
description = "Since v0.6, LionClaw's everyday command path has become a project operator console around real agent CLIs."
draft = true
+++

The most important product decision after v0.6 was not to add a new top-level
workflow. It was to make the existing one obvious.

```sh
lionclaw run
```

That command is the everyday path. It should be where the operator can start a
turn, watch what is happening, interrupt when needed, and understand which
project, instance, runtime, session, boundary, and audit context they are
touching.

Before this work, `run` was still too close to a line prompt. That was useful
for proving the runtime path, but it did not match the shape of real agent
work. Real turns are long. They stream. They change files. They can need native
runtime controls. They can be interrupted. They happen inside a project with
state, not inside an isolated chat box.

So `lionclaw run` became the project operator console.

The Codex-assisted design thread behind this was not about making the terminal
flashier. It kept coming back to a stricter product rule: LionClaw needs one
obvious everyday command path, and that path should reveal the boundary instead
of hiding it. Live Codex smoke work exposed the places where project roots,
daemon reuse, session reuse, runtime controls, and interrupted turns could blur
together. The console work is the product answer to that scar tissue.

The console keeps the durable turn flow in one place: prompt input, transcript,
live runtime status, file-change feedback, and the current answer stream. The
surrounding panes show project objects and inspectable context without making
the operator leave the turn surface. On wide terminals, the project list,
inspector, and changed files can stay visible beside the active conversation.
On narrower terminals, controls can take focus without moving the main Run
surface out from under the operator.

The point is not to imitate a web dashboard in the terminal. The point is to
make the local boundary legible while the agent works.

## Run is for running

`run` is intentionally not a setup wizard.

If a project is missing setup, the console shows launch blockers. It does not
create projects, configure runtimes, install skills, rewrite channel bindings,
or run repair flows. That stays with `doctor`, explicit commands, and operator
intent.

That split matters. LionClaw is a small trusted core around real runtimes. The
command that runs a turn should not quietly mutate the world to make the turn
possible. It should tell you what is missing, then wait for an explicit repair.

For scripts and non-terminal use, the line-oriented path still exists:

```sh
lionclaw run --plain
```

When standard input is not a terminal, LionClaw stays on the plain path
automatically.

## Runtime controls stay native

The console also forced a sharper distinction between LionClaw controls and
runtime controls.

LionClaw reserves `/lionclaw ...` for LionClaw-owned actions such as continue,
retry, reset, and exit. Other first-column slash commands are routed as native
runtime controls. That lets a Codex control like `/compact` remain a Codex
control instead of being stored as ordinary prompt text or reimplemented inside
LionClaw.

Those control turns are still recorded. They are not model-visible history
unless the runtime chooses to make them so. The kernel audits routing, start,
finish, and outcome so the operator can see what happened without LionClaw
pretending to own every runtime command.

Cancellation got the same treatment. Active turns can be interrupted through
the runtime adapter. Queued channel turns can be terminalized before a worker
claims them. The terminal events come from the owner of the state transition,
not from whichever caller happened to ask first.

## The boundary became more inspectable

The console work also made runtime boundaries more concrete.

Runtime profiles can now carry explicit persistent mounts:

```sh
lionclaw runtime mount add codex docs --source /absolute/docs
lionclaw runtime mount list codex
lionclaw runtime mount remove codex docs
```

Mounts default to read-only. LionClaw rejects project metadata, instance state,
reserved runtime paths, invalid targets, and bind arguments that cannot be
represented safely by the OCI backend. The operator gets a durable profile
setting, not an accidental shell environment.

The same period added a Rust-enabled development runtime image while keeping
the default runtime image focused on everyday assistant work. That is the right
shape: the default assistant boundary stays small, and the heavier development
toolchain is opt-in.

## Why this is the right surface

Agents do not only answer. They inspect, edit, run tools, call native controls,
touch files, and sometimes need to be stopped.

If that work is going to happen beside a real project, the everyday surface has
to show more than a prompt and a stream of text. It has to show the local
context around the turn.

That is what `lionclaw run` is becoming: one obvious command path, with the
agent harness still doing the agent work and LionClaw keeping the project-side
boundary visible.

Build notes: this post is extracted from the v0.6 follow-up work in
[#71](https://github.com/moshthepitt/lionclaw/pull/71),
[#76](https://github.com/moshthepitt/lionclaw/pull/76),
[#83](https://github.com/moshthepitt/lionclaw/pull/83),
[#84](https://github.com/moshthepitt/lionclaw/pull/84),
[#90](https://github.com/moshthepitt/lionclaw/pull/90),
[#96](https://github.com/moshthepitt/lionclaw/pull/96), and
[#97](https://github.com/moshthepitt/lionclaw/pull/97).
