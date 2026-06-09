use std::process::ExitCode;

use anyhow::{bail, Result};
use clap::{ArgAction, Subcommand};
use serde::Serialize;
use serde_json::json;

use crate::{
    protocol::ProjectedContextClass,
    store::{MemoryPatch, PrivateContextStore},
};

const DEFAULT_SCOPE: &str = "global";

#[derive(Debug, Subcommand)]
pub(crate) enum ContextCommand {
    #[command(subcommand)]
    Profile(ProfileTargetCommand),
    #[command(subcommand)]
    Memory(MemoryCommand),
    Operations {
        #[arg(long)]
        item_id: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileTargetCommand {
    #[command(subcommand)]
    Assistant(ProfileCommand),
    #[command(subcommand)]
    User(ProfileCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    Set {
        slot: String,
        body: String,
        #[arg(long, default_value = DEFAULT_SCOPE)]
        scope: String,
    },
    List {
        #[arg(long)]
        scope: Option<String>,
    },
    Show {
        slot: String,
        #[arg(long, default_value = DEFAULT_SCOPE)]
        scope: String,
    },
    Delete {
        slot: String,
        #[arg(long, default_value = DEFAULT_SCOPE)]
        scope: String,
    },
    History {
        slot: String,
        #[arg(long, default_value = DEFAULT_SCOPE)]
        scope: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum MemoryCommand {
    Remember {
        body: String,
        #[arg(long, default_value = DEFAULT_SCOPE)]
        scope: String,
        #[arg(long)]
        title: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, action = ArgAction::SetTrue)]
        pinned: bool,
    },
    Search {
        query: String,
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    List {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    Show {
        id: String,
    },
    Update {
        id: String,
        #[arg(long, conflicts_with = "clear_title")]
        title: Option<String>,
        #[arg(long, action = ArgAction::SetTrue)]
        clear_title: bool,
        #[arg(long)]
        body: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long, default_value_t = false)]
        replace_tags: bool,
        #[arg(long)]
        priority: Option<i64>,
        #[arg(long)]
        pinned: Option<bool>,
    },
    Delete {
        id: String,
    },
    History {
        id: String,
    },
}

pub(crate) async fn run(command: ContextCommand) -> Result<ExitCode> {
    let store = PrivateContextStore::open_from_env().await?;
    match command {
        ContextCommand::Profile(target) => run_profile(&store, target).await?,
        ContextCommand::Memory(command) => run_memory(&store, command).await?,
        ContextCommand::Operations { item_id } => {
            write_json(&store.operation_log(item_id.as_deref()).await?)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn run_profile(store: &PrivateContextStore, target: ProfileTargetCommand) -> Result<()> {
    let (class, command) = match target {
        ProfileTargetCommand::Assistant(command) => {
            (ProjectedContextClass::AssistantProfile, command)
        }
        ProfileTargetCommand::User(command) => (ProjectedContextClass::UserProfile, command),
    };
    match command {
        ProfileCommand::Set { slot, body, scope } => {
            write_json(&store.set_profile(class, &scope, &slot, &body).await?)?;
        }
        ProfileCommand::List { scope } => {
            write_json(&store.list_profiles(class, scope.as_deref()).await?)?;
        }
        ProfileCommand::Show { slot, scope } => {
            write_json(&store.show_profile(class, &scope, &slot).await?)?;
        }
        ProfileCommand::Delete { slot, scope } => {
            let deleted = store.delete_profile(class, &scope, &slot).await?;
            write_json(&json!({
                "deleted": deleted.is_some(),
                "item": deleted,
            }))?;
        }
        ProfileCommand::History { slot, scope } => {
            write_json(&store.profile_history(class, &scope, &slot).await?)?;
        }
    }
    Ok(())
}

async fn run_memory(store: &PrivateContextStore, command: MemoryCommand) -> Result<()> {
    match command {
        MemoryCommand::Remember {
            body,
            scope,
            title,
            tags,
            priority,
            pinned,
        } => {
            write_json(
                &store
                    .remember_memory(&scope, title.as_deref(), &body, &tags, priority, pinned)
                    .await?,
            )?;
        }
        MemoryCommand::Search {
            query,
            scope,
            limit,
        } => {
            write_json(&store.search_memory(&query, scope.as_deref(), limit).await?)?;
        }
        MemoryCommand::List { scope, limit } => {
            write_json(&store.list_memory(scope.as_deref(), limit).await?)?;
        }
        MemoryCommand::Show { id } => {
            write_json(&store.show_memory(&id).await?)?;
        }
        MemoryCommand::Update {
            id,
            title,
            clear_title,
            body,
            tags,
            replace_tags,
            priority,
            pinned,
        } => {
            if !tags.is_empty() && !replace_tags {
                bail!("memory update tags replace the full tag set; pass --replace-tags");
            }
            let patch = MemoryPatch {
                title: title.map(Some).or(clear_title.then_some(None)),
                body,
                tags: replace_tags.then_some(tags),
                priority,
                pinned,
            };
            write_json(&store.update_memory(&id, patch).await?)?;
        }
        MemoryCommand::Delete { id } => {
            let deleted = store.delete_memory(&id).await?;
            write_json(&json!({
                "deleted": deleted.is_some(),
                "item": deleted,
            }))?;
        }
        MemoryCommand::History { id } => {
            write_json(&store.memory_history(&id).await?)?;
        }
    }
    Ok(())
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, value)?;
    use std::io::Write as _;
    writeln!(&mut handle)?;
    Ok(())
}
