//! Runs the `reth bench` command, sending only forkchoiceUpdated calls, without newPayload.

use crate::{
    bench::{
        context::BenchContext,
        output::{
            ForkchoiceResult, TotalGasOutput, TotalGasRow, GAS_OUTPUT_SUFFIX,
            FORKCHOICE_OUTPUT_SUFFIX,
        },
    },
    valid_payload::{call_forkchoice_updated},
};
use alloy_provider::Provider;
use alloy_rpc_types_engine::ForkchoiceState;
use clap::Parser;
use csv::Writer;
use reth_cli_runner::CliContext;
use reth_node_core::args::BenchmarkArgs;
use reth_node_api::EngineApiMessageVersion;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// `reth benchmark forkchoice-only` command
#[derive(Debug, Parser)]
pub struct Command {
    /// The RPC url to use for getting data.
    #[arg(long, value_name = "RPC_URL", verbatim_doc_comment)]
    rpc_url: String,

    #[command(flatten)]
    benchmark: BenchmarkArgs,
}

impl Command {
    /// Execute `benchmark forkchoice-only` command
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        let BenchContext {
            benchmark_mode,
            block_provider,
            auth_provider,
            mut next_block,
            is_optimism: _,
            chain_id: _,
        } = BenchContext::new(&self.benchmark, self.rpc_url).await?;

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1000);
        tokio::task::spawn(async move {
            while benchmark_mode.contains(next_block) {
                let block_res = block_provider.get_block_by_number(next_block.into()).full().await;
                let block = block_res.unwrap().unwrap();
                let header = block.header.clone();

                let head_block_hash = header.hash;
                let safe_block_hash =
                    block_provider.get_block_by_number(header.number.saturating_sub(32).into());

                let finalized_block_hash =
                    block_provider.get_block_by_number(header.number.saturating_sub(64).into());

                let (safe, finalized) = tokio::join!(safe_block_hash, finalized_block_hash,);

                let safe_block_hash = safe.unwrap().expect("safe block exists").header.hash;
                let finalized_block_hash =
                    finalized.unwrap().expect("finalized block exists").header.hash;

                next_block += 1;
                // send block info to receiver
                if sender
                    .send((
                        header,
                        head_block_hash,
                        safe_block_hash,
                        finalized_block_hash,
                    ))
                    .await
                    .is_err()
                {
                    // if receiver is closed, stop the block producer task
                    tracing::info!("Receiver closed, stopping block producer task");
                    break;
                }
            }
        });

        // put results in a summary vec so they can be printed at the end
        let mut results = Vec::new();
        let total_benchmark_duration = Instant::now();
        let mut total_wait_time = Duration::ZERO;

        while let Some((header, head, safe, finalized)) = {
            let wait_start = Instant::now();
            let result = receiver.recv().await;
            total_wait_time += wait_start.elapsed();
            result
        } {
            // just put gas used here
            let gas_used = header.gas_used;
            let block_number = header.number;

            debug!(
                target: "reth-bench",
                number=?header.number,
                "Sending forkchoice update to engine",
            );

            // construct fcu to call
            let forkchoice_state = ForkchoiceState {
                head_block_hash: head,
                safe_block_hash: safe,
                finalized_block_hash: finalized,
            };

            let start = Instant::now();
            
            // Retry loop for SYNCING status
            loop {
                match call_forkchoice_updated(&auth_provider, EngineApiMessageVersion::V1, forkchoice_state, None).await {
                    Ok(_) => break,
                    Err(e) => {
                        let error_msg = format!("{:?}", e);
                        if error_msg.contains("SYNCING") {
                            tracing::debug!("Node is syncing, waiting 200ms before retry...");
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            continue;
                        } else {
                            tracing::error!("Engine API error: {}", error_msg);
                            return Err(e.into());
                        }
                    }
                }
            }

            let forkchoice_result = ForkchoiceResult { gas_used, latency: start.elapsed() };
            info!(%forkchoice_result);

            // current duration since the start of the benchmark minus the time
            // waiting for blocks
            let current_duration = total_benchmark_duration.elapsed() - total_wait_time;

            // record the current result
            let row = TotalGasRow { block_number, gas_used, time: current_duration };
            results.push((row, forkchoice_result));
        }

        let (gas_output_results, forkchoice_results): (_, Vec<ForkchoiceResult>) =
            results.into_iter().unzip();

        // write the csv output to files
        if let Some(path) = self.benchmark.output {
            // first write the forkchoice results to a file
            let output_path = path.join(FORKCHOICE_OUTPUT_SUFFIX);
            info!("Writing forkchoiceUpdated call latency output to file: {:?}", output_path);
            let mut writer = Writer::from_path(output_path)?;
            for result in forkchoice_results {
                writer.serialize(result)?;
            }
            writer.flush()?;

            // now write the gas output to a file
            let output_path = path.join(GAS_OUTPUT_SUFFIX);
            info!("Writing total gas output to file: {:?}", output_path);
            let mut writer = Writer::from_path(output_path)?;
            for row in &gas_output_results {
                writer.serialize(row)?;
            }
            writer.flush()?;

            info!("Finished writing benchmark output files to {:?}.", path);
        }

        // accumulate the results and calculate the overall Ggas/s
        let gas_output = TotalGasOutput::new(gas_output_results);
        info!(
            total_duration=?gas_output.total_duration,
            total_gas_used=?gas_output.total_gas_used,
            blocks_processed=?gas_output.blocks_processed,
            "Total Ggas/s: {:.4}",
            gas_output.total_gigagas_per_second()
        );

        Ok(())
    }
}