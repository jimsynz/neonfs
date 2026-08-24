//! Block device management commands.
//!
//! # Attaching is not symmetrical
//!
//! A ublk device is created by the kernel of the host running the block
//! target, so `attach` over ublk makes a device appear **on that node** —
//! not on the machine running this CLI, unless they are the same machine.
//! NBD is the other way round: the client runs `nbd-client`, and the device
//! appears wherever that ran.
//!
//! So `attach` performs the first kind and reports the second. Asking for
//! `--frontend nbd`, or `auto` on a node that cannot serve ublk, prints the
//! endpoint to dial and says plainly that nothing was attached. A script
//! that needs a device path asks for `--frontend ublk`, which fails naming
//! the check that failed rather than answering something else.

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{BlockAttachment, BlockDetachResult, BlockDeviceInfo, BlockNodeFrontends};
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Term};

/// Block device management subcommands
#[derive(Debug, Subcommand)]
pub enum BlockCommand {
    /// Attach a volume as a block device
    ///
    /// Over ublk the device is created on the block node that serves it, so
    /// `/dev/ublkbN` appears there rather than here. Over NBD nothing is
    /// attached: the endpoint to dial is printed instead, because the device
    /// appears wherever `nbd-client` runs.
    Attach {
        /// Export name: `<volume>` or `<volume>:<path>`
        export: String,

        /// Frontend to use. `ublk` fails if unavailable, naming which check
        /// failed; `auto` reports the NBD endpoint instead of failing.
        #[arg(long, default_value = "auto", value_parser = ["auto", "ublk", "nbd"])]
        frontend: String,
    },

    /// Detach a ublk device
    ///
    /// Idempotent. NBD devices are not detachable from here — they belong to
    /// whichever host ran `nbd-client`.
    Detach {
        /// Export name: `<volume>` or `<volume>:<path>`
        export: String,
    },

    /// List attached block devices across the cluster
    List,

    /// Report which frontends each block node can serve
    Frontends,
}

impl BlockCommand {
    /// Execute the block command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            BlockCommand::Attach { export, frontend } => self.attach(export, frontend, format),
            BlockCommand::Detach { export } => self.detach(export, format),
            BlockCommand::List => self.list(format),
            BlockCommand::Frontends => self.frontends(format),
        }
    }

    fn attach(&self, export: &str, frontend: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "block_attach",
                vec![binary(export), binary(frontend)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let attachment = BlockAttachment::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&attachment)?),
            OutputFormat::Table => print_attachment(&attachment),
        }
        Ok(())
    }

    fn detach(&self, export: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "block_detach",
                vec![binary(export)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let detach = BlockDetachResult::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&detach)?),
            OutputFormat::Table => print_detach(&detach),
        }
        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "list_block_devices", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let devices: Result<Vec<BlockDeviceInfo>> = term_to_list(&unwrap_ok_tuple(result)?)?
            .into_iter()
            .map(BlockDeviceInfo::from_term)
            .collect();
        let devices = devices?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&devices)?),
            OutputFormat::Table => print_devices(&devices)?,
        }
        Ok(())
    }

    fn frontends(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "block_frontends", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let nodes: Result<Vec<BlockNodeFrontends>> = term_to_list(&unwrap_ok_tuple(result)?)?
            .into_iter()
            .map(BlockNodeFrontends::from_term)
            .collect();
        let nodes = nodes?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&nodes)?),
            OutputFormat::Table => print_frontends(&nodes)?,
        }
        Ok(())
    }
}

fn binary(value: &str) -> Term {
    Term::Binary(Binary {
        bytes: value.as_bytes().to_vec(),
    })
}

// The node is named on every line because it is the part an operator gets
// wrong: a ublk device is only usable on its own host.
fn print_attachment(attachment: &BlockAttachment) {
    match attachment.device_path.as_deref() {
        Some(path) => {
            println!(
                "✓ '{}' attached over {} on {}",
                attachment.export, attachment.frontend, attachment.node
            );
            println!("  Device: {} (on {})", path, attachment.node);
        }
        None => {
            println!(
                "! '{}' was not attached: {} devices are attached by the client",
                attachment.export, attachment.frontend
            );

            if let Some(endpoint) = &attachment.endpoint {
                println!("  Endpoint: {}:{}", endpoint.host, endpoint.port);
                println!(
                    "  Run: nbd-client -N {} {} {} /dev/nbdX -b 4096 -persist",
                    attachment.export, endpoint.host, endpoint.port
                );
                println!("  The device appears on whichever host runs that.");
            }

            if let Some(reason) = &attachment.reason {
                println!("  ublk was not used because: {}", reason);
            }
        }
    }
}

fn print_detach(detach: &BlockDetachResult) {
    if detach.detached.is_empty() {
        println!("Nothing attached for '{}'", detach.export);
        return;
    }

    for entry in &detach.detached {
        if entry.detached {
            println!("✓ '{}' detached from {}", detach.export, entry.node);
        } else {
            println!(
                "✗ '{}' could not be detached from {}: {}",
                detach.export,
                entry.node,
                entry.reason.as_deref().unwrap_or("unknown")
            );
        }
    }
}

fn print_devices(devices: &[BlockDeviceInfo]) -> Result<()> {
    if devices.is_empty() {
        println!("No attached block devices");
        return Ok(());
    }

    let mut tbl = table::Table::new(vec![
        "NODE".to_string(),
        "EXPORT".to_string(),
        "FRONTEND".to_string(),
        "HOLDERS".to_string(),
    ]);

    for device in devices {
        tbl.add_row(vec![
            device.node.clone(),
            device.export.clone(),
            device.frontend.clone(),
            device
                .holders
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }

    print!("{}", tbl.render()?);
    Ok(())
}

fn print_frontends(nodes: &[BlockNodeFrontends]) -> Result<()> {
    if nodes.is_empty() {
        println!("No block nodes");
        return Ok(());
    }

    let mut tbl = table::Table::new(vec![
        "NODE".to_string(),
        "FRONTENDS".to_string(),
        "UBLK UNAVAILABLE BECAUSE".to_string(),
    ]);

    for node in nodes {
        tbl.add_row(vec![
            node.node.clone(),
            node.frontends.join(", "),
            node.ublk_unavailable
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }

    print!("{}", tbl.render()?);
    Ok(())
}
