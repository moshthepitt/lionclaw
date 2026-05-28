+++
title = "The run command became a control room"
date = 2026-05-25
description = "lionclaw run started as the obvious place to talk to an agent. It became the project operator console because LionClaw is a control plane around real agent harnesses, not another harness."

[extra]
standfirst = "At first, <code>lionclaw run</code> looked like it wanted to be a chat box. That was the wrong ceiling. LionClaw is the local control plane around real agent harnesses, so the everyday command has to feel more like a control room."
hero_image = "/assets/lionclaw-run-console.png"
hero_alt = "LionClaw run console showing project sessions, transcript, inspector, composer, and command help"
hero_width = 1890
hero_height = 969
hero_caption = "<code>lionclaw run</code> keeps conversation, runtime state, project context, inspector panes, and changed files in one terminal."
+++

At first `lionclaw run` looked like it wanted to become a chat box.

That was not a bad place to start.

When you use an agent, the first thing you need is conversation. You need
somewhere to explain what you want. You need to go back and forth. You need to
make a plan, change your mind, review work, and tell the agent that no, that is
not quite what you meant.

So yes, chat matters.

But chat is not enough.

If LionClaw was another agent harness, `run` would probably keep moving in that
direction. Better prompt editing. Better transcript. Better planning. Better
review. A more polished place to talk to your agent and watch it do things.

That is a real product. It is just not this product.

LionClaw is not trying to become Codex. It is not trying to become OpenCode. It
should not become a thinner version of whatever agent UI happens to be popular
this month.

In the README I put the important thing plainly: Codex is still Codex. OpenCode
is still OpenCode. LionClaw is the local control plane around them.

That sentence decides what `run` has to become.

## The wrong ceiling

A chat box is a very small ceiling for something that can act inside a real
project.

An agent is doing more than answering questions. It is reading files, running
tools, editing code, using runtime state, continuing sessions, producing drafts,
and sometimes touching credentials. It may be Codex today, OpenCode tomorrow,
Claude Code when that makes sense, or something better that does not exist yet.

The harness can change. The project home should not.

That is the whole point of LionClaw.

The model may be commercial. Fine. The harness may be built by someone else.
Also fine. Use the strongest tool available. But the boundary around the work
should belong to you: the project it runs in, the state it sees, the sessions it
continues, the skills and mounts it receives, the channels that can reach it,
and the audit trail it leaves behind.

So `lionclaw run` cannot just be a pleasant stream of text. It has to show the
room.

## What the room shows

The everyday command is still simple:

```sh
lionclaw run
```

In a terminal, that opens the project operator console.

The conversation is still there. It has to be. But it is only one surface.

When I am running an agent inside a project, I want to see:

- which project instance I am touching
- which runtime is doing the work
- which session is being continued
- whether the current turn is alive or stuck
- what the runtime is doing right now
- what files are changing
- what boundary the runtime is inside
- what controls are available without pretending every control belongs to LionClaw

This is not about making the terminal look impressive.

When an agent is acting, the local working context should be visible.

The current console keeps the prompt and durable transcript together. Runtime
status, reasoning, command activity, progress, and file changes are visible as
live activity or control-pane detail instead of being dumped into the transcript
as if they were part of the conversation.

That distinction matters.

I want to read the conversation later and understand what was said. I also want
to know what the runtime was doing while the turn was active. Those are related
things, but they are not the same thing.

## Control plane, not harness

This is the split I keep coming back to:

The runtime does the agent work.

LionClaw owns the boundary.

That means LionClaw decides where the runtime runs, what work root it sees, what
runtime state is mounted, what draft area exists, what secrets are staged, what
session is durable, and what gets recorded.

It also means LionClaw should not swallow every native runtime behaviour and
pretend it invented it.

If Codex has a control like `/compact`, that should remain a Codex control. If
OpenCode has its own behaviour, that should remain OpenCode's behaviour.
LionClaw can route and record the turn without turning itself into a bad clone
of every harness.

That is why this is not agent chat with extra panels.

It is a control plane with a conversation surface.

## Projects need operators

A LionClaw project can have configured instances. Each instance has its own
home, runtime config, sessions, logs, installed skills, runtime cache, and
assistant-home continuity.

That is where the control-room idea starts to matter.

Today, the honest claim is narrower than the dream. `run` is a project operator
console. It can render the configured project instances. It can switch to
another configured instance when no turn is active. That switch reads the other
instance's home, work root, runtime config, sessions, and audit scope. It does
not secretly mutate the project to make things work.

Good.

I do not want magic here. I want to understand why I am looking at this agent,
this runtime, this session, and this work root.

Over time, this should become richer. I want audit panes that are actually
useful. I want drafts produced by agents to be easy to inspect, promote, or
discard. I want logs, jobs, channel traffic, and team state to have natural
places to live. I want to see Codex doing one kind of work, OpenCode doing
another, and some future harness doing whatever strange thing it is good at.

But the rule should stay the same.

The harnesses do the work. LionClaw gives the project a local home and an
operator surface.

## Run should not repair the world

One thing I do not want is a `run` command that quietly rewrites everything
before it starts.

If setup is wrong, `doctor` should say so. If a runtime is missing, the operator
should configure it explicitly. If a channel needs approval, that should be a
deliberate act. If a mount is added, it should be remembered as runtime-profile
state, not accidentally inherited from whatever shell happened to start the
process.

For scripts and non-terminal use, the plain path still exists:

```sh
lionclaw run --plain
```

That is fine. Scripts need boring text.

Humans doing serious agent work need more context.

## Why I care

I am building LionClaw because I do not want the future of agents to become
another rented platform.

We already did that with the web. The tools became convenient. Then the memory,
logs, workflows, credentials, and distribution all moved somewhere else. After a
while you are just renting your own life back through someone else's interface.

Agents make that problem sharper.

If agents are going to do real work, then the home around that work matters.
The transcript matters. The runtime state matters. The files matter. The audit
trail matters. The ability to swap out the harness matters.

`lionclaw run` is where that idea becomes visible.

It did not become a bigger chat box.

It became the place where conversation, runtime state, project state, file
changes, sessions, and boundary controls start to meet.

That is the shape of LionClaw.

Not another harness.

A local control plane for real harnesses.
