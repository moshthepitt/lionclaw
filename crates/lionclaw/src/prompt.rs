//! Engine-owned prompt assembly: the engine writes the skeleton (output-
//! semantics contract + handoff protocol + guardrails), the mission type fills the
//! content slot, the mission adds live context. Enforced exclusion: a
//! judge's prompt never includes a producer's narrative prose — verdict
//! roles see the contract and the artifact, not the worker's story.

use crate::mission_type::RoleDefinition;
use crate::model::{Assertion, OutputSemantics};

pub struct PromptContext<'a> {
    pub objective: &'a str,
    pub task_body: &'a str,
    /// The contract assertions this dispatch targets (id + prose).
    pub targets: &'a [&'a Assertion],
    /// Reports from this task's cleared dependencies, threaded in for
    /// producing roles. **Excluded for verdict roles** — a judge sees the
    /// artifact and the contract, never the producer's narrative
    /// (fresh-context).
    pub upstream_reports: &'a [String],
}

pub fn assemble_role_prompt(role: &RoleDefinition, ctx: &PromptContext<'_>) -> String {
    let mut prompt = String::new();
    prompt.push_str(skeleton(role.output));
    prompt.push_str("\n\n## Role\n\n");
    prompt.push_str(&role.prompt_body);
    prompt.push_str("\n\n## Mission objective\n\n");
    prompt.push_str(ctx.objective);
    if !ctx.task_body.is_empty() {
        prompt.push_str("\n\n## Task\n\n");
        prompt.push_str(ctx.task_body);
    }
    if !ctx.targets.is_empty() {
        prompt.push_str("\n\n## Contract assertions in scope\n\n");
        for assertion in ctx.targets {
            prompt.push_str(&format!("- {}: {}\n", assertion.id, assertion.prose));
        }
    }
    // Fresh-context invariant: verdict roles never receive producer prose.
    if role.output != OutputSemantics::EmitsVerdict && !ctx.upstream_reports.is_empty() {
        prompt.push_str("\n\n## Handoffs from upstream tasks\n\n");
        for report in ctx.upstream_reports {
            prompt.push_str(&format!("- {report}\n"));
        }
    }
    prompt
}

/// Context for a planning role (research / draft / red-team / author). A
/// **separate** assembler from `assemble_role_prompt` so a producer's prose can
/// never structurally reach an execution judge: planning has no verdict roles,
/// and execution judges are only ever built by `assemble_role_prompt`.
pub struct PlanningPromptContext<'a> {
    pub objective: &'a str,
    /// The mission type's playbook (its method), if any.
    pub playbook: Option<&'a str>,
    /// The oracles the author may bind assertions to.
    pub oracle_inventory: &'a [String],
    pub task_body: &'a str,
    /// Reports from this planning node's cleared dependencies.
    pub upstream_reports: &'a [String],
}

pub fn assemble_planning_prompt(role: &RoleDefinition, ctx: &PlanningPromptContext<'_>) -> String {
    let mut prompt = String::new();
    prompt.push_str(skeleton(role.output));
    prompt.push_str("\n\n## Role\n\n");
    prompt.push_str(&role.prompt_body);
    prompt.push_str("\n\n## Mission objective\n\n");
    prompt.push_str(ctx.objective);
    if let Some(playbook) = ctx.playbook {
        prompt.push_str("\n\n## Playbook\n\n");
        prompt.push_str(playbook);
    }
    if !ctx.oracle_inventory.is_empty() {
        prompt.push_str("\n\n## Available oracles\n\n");
        prompt
            .push_str("Bind an assertion to one of these to make it authoritatively checkable:\n");
        for oracle in ctx.oracle_inventory {
            prompt.push_str(&format!("- {oracle}\n"));
        }
    }
    if !ctx.task_body.is_empty() {
        prompt.push_str("\n\n## Task\n\n");
        prompt.push_str(ctx.task_body);
    }
    if !ctx.upstream_reports.is_empty() {
        prompt.push_str("\n\n## Upstream planning reports\n\n");
        for report in ctx.upstream_reports {
            prompt.push_str(&format!("- {report}\n"));
        }
    }
    prompt
}

/// The engine skeleton per output semantics — a total match; adding a
/// variant without a contract is a compile error.
fn skeleton(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::ProducesArtifact => PRODUCES_ARTIFACT_SKELETON,
        OutputSemantics::EmitsVerdict => EMITS_VERDICT_SKELETON,
        OutputSemantics::ProducesReport => PRODUCES_REPORT_SKELETON,
        OutputSemantics::ProposesPlan => PROPOSES_PLAN_SKELETON,
    }
}

const PRODUCES_ARTIFACT_SKELETON: &str = "\
You are one role in an engineering mission run by an engine that independently
verifies results; your report is never taken on faith.

Your workspace is mounted read-write at /workspace. Work only there.

When you are finished you MUST:
1. Commit ALL changes in /workspace with a clear message (the engine records
   the commit, not your description of it; uncommitted work is discarded).
2. Write /mission/handoff/handoff.json exactly like:
   {\"schema\": \"lionclaw.mission.work-handoff.v1\",
    \"type\": \"work\",
    \"done\": true,
    \"report\": {\"kind\": \"inline\", \"text\": \"<what you did and why>\"},
    \"request_attention\": false}
   Set done=false if you could not complete the task; set
   request_attention=true only if a human must look before the mission
   continues.";

const EMITS_VERDICT_SKELETON: &str = "\
You are an independent validator in an engineering mission. Judge only the
artifact in front of you against the contract assertions listed below; you
have deliberately not been shown the author's own account of the work.

Your workspace is mounted READ-ONLY at /workspace. You cannot and must not
modify it.

When you are finished you MUST write /mission/handoff/handoff.json exactly like:
   {\"schema\": \"lionclaw.mission.validate-handoff.v1\",
    \"type\": \"validate\",
    \"done\": true,
    \"report\": {\"kind\": \"inline\", \"text\": \"<your findings>\"},
    \"items\": [{\"item_id\": \"<ASSERTION-ID>\", \"passed\": false}],
    \"passed\": false,
    \"request_attention\": false}
Report one item per contract assertion in scope, with your honest verdict.
Your verdicts are advisory: they route work, they can never mark the mission
verified.";

const PRODUCES_REPORT_SKELETON: &str = "\
You are a planning role in an engineering mission. Read the workspace (mounted
read-only at /workspace) and produce the report the task asks for — research,
a draft plan, or an adversarial critique. You do not modify anything.

When you are finished you MUST write /mission/handoff/handoff.json exactly like:
   {\"schema\": \"lionclaw.mission.work-handoff.v1\",
    \"type\": \"work\",
    \"done\": true,
    \"report\": {\"kind\": \"inline\", \"text\": \"<your report>\"},
    \"request_attention\": false}";

const PROPOSES_PLAN_SKELETON: &str = "\
You are the planning author. Read the workspace (mounted read-only at
/workspace) and the upstream reports, then propose the mission's contract and
task DAG. You do not modify anything; your deliverable is the proposal itself.

A proposal is a contract of falsifiable assertions plus a task DAG that covers
them. Rules the engine enforces (an invalid proposal is rejected):
- assertion ids match ^[A-Z][A-Z0-9-]+$ ; task ids match ^[A-Za-z][A-Za-z0-9_-]*$
- each assertion is covered by exactly one `work` task (via its `targets`)
- an assertion an oracle can check should bind that oracle by name; under a
  `verified` mission type EVERY assertion must bind an oracle
- `validate` tasks add an independent reviewer; `gate` tasks (no role, no body)
  gate a set of assertions behind their validators
- the DAG is acyclic and every dependency resolves

When you are finished you MUST write /mission/handoff/handoff.json exactly like:
   {\"schema\": \"lionclaw.mission.plan-handoff.v1\",
    \"type\": \"plan\",
    \"done\": true,
    \"report\": {\"kind\": \"inline\", \"text\": \"<why this contract>\"},
    \"proposal\": {\"assertions\": [{\"id\": \"TESTS-PASS\", \"prose\": \"...\",
                                    \"oracle\": \"cargo-test\"}],
                   \"tasks\": [{\"id\": \"fix\", \"kind\": \"work\", \"body\": \"...\",
                               \"targets\": [\"TESTS-PASS\"], \"role\": \"implementer\",
                               \"depends_on\": []}]},
    \"request_attention\": false}";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RoleName;

    fn role(output: OutputSemantics) -> RoleDefinition {
        RoleDefinition {
            name: RoleName::new("r").unwrap(),
            output,
            runtime: None,
            network: false,
            secrets: false,
            skills: Vec::new(),
            prompt_body: "role body".to_string(),
        }
    }

    fn ctx<'a>(upstream: &'a [String]) -> PromptContext<'a> {
        PromptContext {
            objective: "obj",
            task_body: "do it",
            targets: &[],
            upstream_reports: upstream,
        }
    }

    #[test]
    fn worker_prompt_includes_upstream_reports() {
        let upstream = vec!["the planner said: change add()".to_string()];
        let prompt =
            assemble_role_prompt(&role(OutputSemantics::ProducesArtifact), &ctx(&upstream));
        assert!(prompt.contains("Handoffs from upstream tasks"));
        assert!(prompt.contains("the planner said"));
    }

    #[test]
    fn judge_prompt_excludes_producer_prose_fresh_context() {
        let upstream = vec!["the implementer said: I changed add() to add".to_string()];
        let prompt = assemble_role_prompt(&role(OutputSemantics::EmitsVerdict), &ctx(&upstream));
        // A judge must never see the producer's narrative.
        assert!(!prompt.contains("the implementer said"));
        assert!(!prompt.contains("Handoffs from upstream tasks"));
    }
}
