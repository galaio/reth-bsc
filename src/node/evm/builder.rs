use crate::{BscPrimitives, hardforks::BscHardforks, node::evm::{assembler::{BscBlockAssembler, BscBlockAssemblerInput}, config::{BscBlockExecutionCtx, BscBlockExecutorFactory, BscExecutionSharedCtx, STATE_ROOT_ALGORITHM}, executor::BscBlockExecutor, factory::BscEvmFactory, pre_execution::{TURN_LENGTH_CACHE, VALIDATOR_CACHE}}};
use alloy_primitives::BlockHash;
use reth_engine_primitives::{BSCEngineMessageError};
use reth_engine_tree::{engine::EngineApiRequest, tree::{PayloadProcessor, sparse_trie::StateRootComputeOutcome}};
use reth_engine_tree::tree::{CustomRequestMessage, PayloadHandle, CustomParallelCtx};
use reth_evm::{ConfigureEvm, execute::{BlockBuilder, BlockBuilderOutcome, BlockExecutionError, ExecutorTx, WithTxEnv}};
use crate::evm::transaction::BscTxEnv;
use alloy_consensus::{EthereumTxEnvelope, TxEip4844};
use alloy_evm::eth::receipt_builder::ReceiptBuilder;
use reth_node_builder::rpc::EngineApiTx;
use reth::builder::NodeAdapter;
use reth_primitives_traits::{HeaderTy, NodePrimitives, Recovered, RecoveredBlock, SealedHeader, SignerRecoverable, TxTy};
use reth_provider::StateProvider;
use reth_trie_parallel::root::ParallelStateRoot;
use reth_trie_common::TrieInput;
use revm::database::{State, states::bundle_state::BundleRetention};
use alloy_evm::{Evm, block::BlockExecutor};
use alloy_evm::block::StateChangeSource;
use reth_evm::OnStateHook;
use reth_chainspec::{EthChainSpec, EthereumHardforks, Hardforks};
use tokio::sync::oneshot;
use crate::node::BscNode;


/// rewrite BasicBlockBuilder, mainly about the finish() trait.
/// add system txs to sealed block.
pub struct BscBlockBuilder<'a, EVM, Spec, R, CEvm>
where
    R: ReceiptBuilder,
    Spec: EthChainSpec + EthereumHardforks + BscHardforks + Hardforks + Clone,
    CEvm: ConfigureEvm,
{
    /// The block executor used to execute transactions.
    pub executor: BscBlockExecutor<'a, EVM, Spec, R>,
    /// The transactions executed in this block.
    pub transactions: Vec<Recovered<TxTy<BscPrimitives>>>,
    /// The parent block execution context.
    pub ctx: BscBlockExecutionCtx<'a>,
    /// The shared context for block execution.
    pub shared_ctx: BscExecutionSharedCtx,
    /// The sealed parent block header.
    pub parent: &'a SealedHeader<HeaderTy<BscPrimitives>>,
    /// The assembler used to build the block.
    pub assembler: &'a BscBlockAssembler<crate::chainspec::BscChainSpec>,
    /// Payload processor for state root computation.
    pub payload_processor: Option<PayloadProcessor<CEvm>>,
    /// Payload handle for state root computation.
    pub payload_handle: Option<
        PayloadHandle<
            WithTxEnv<
                BscTxEnv,
                Recovered<EthereumTxEnvelope<TxEip4844>>
            >,
            BlockExecutionError
        >
    >,
}

impl<'a, EVM, Spec, R, CEvm> BscBlockBuilder<'a, EVM, Spec, R, CEvm>
where
    R: ReceiptBuilder,
    Spec: EthChainSpec + EthereumHardforks + BscHardforks + Hardforks + Clone,
    CEvm: ConfigureEvm,
{
    pub fn new(
        executor: BscBlockExecutor<'a, EVM, Spec, R>,
        ctx: BscBlockExecutionCtx<'a>,
        shared_ctx: BscExecutionSharedCtx,
        assembler: &'a BscBlockAssembler<crate::chainspec::BscChainSpec>,
        parent: &'a SealedHeader<HeaderTy<BscPrimitives>>,
    ) -> Self {
        Self {
            executor,
            transactions: Vec::new(),
            ctx,
            shared_ctx,
            parent,
            assembler,
            payload_processor: None,
            payload_handle: None,
        }
    }
}

impl<'a, DB, EVM, Spec, R, CEvm> BlockBuilder for BscBlockBuilder<'a, EVM, Spec, R, CEvm>
where
    BscBlockExecutor<'a, EVM, Spec, R>: alloy_evm::block::BlockExecutor<
        Evm: alloy_evm::Evm<
            Spec = <BscEvmFactory as reth_evm::EvmFactory>::Spec,
            HaltReason = <BscEvmFactory as reth_evm::EvmFactory>::HaltReason,
            DB = &'a mut State<DB>,
        >,
        Transaction = <BscPrimitives as NodePrimitives>::SignedTx,
        Receipt = <BscPrimitives as NodePrimitives>::Receipt,
    >,
    DB: reth_evm::Database + 'a,
    R: ReceiptBuilder<Transaction = <BscPrimitives as NodePrimitives>::SignedTx>,
    Spec: EthChainSpec + EthereumHardforks + BscHardforks + Hardforks + Clone,
    R::Transaction: Clone + SignerRecoverable,
    EVM: alloy_evm::Evm,
    CEvm: ConfigureEvm,
{
    type Primitives = BscPrimitives;
    type Executor = BscBlockExecutor<'a, EVM, Spec, R>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        self.executor.apply_pre_execution_changes()
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutorTx<Self::Executor>,
        f: impl FnOnce(
            &revm::context::result::ExecutionResult<<<Self::Executor as alloy_evm::block::BlockExecutor>::Evm as alloy_evm::Evm>::HaltReason>,
        ) -> alloy_evm::block::CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        if let Some(gas_used) =
            self.executor.execute_transaction_with_commit_condition(tx.as_executable(), f)?
        {
            self.transactions.push(tx.into_recovered());
            Ok(Some(gas_used))
        } else {
            Ok(None)
        }
    }

    // fetch assembled_system_txs and add into sealed block.
    fn finish(
        mut self,
        state: impl StateProvider,
    ) -> Result<BlockBuilderOutcome<BscPrimitives>, BlockExecutionError> {
        let finish_start = std::time::Instant::now();
        let (evm, result) = self.executor.finish()?;
        let (db, evm_env) = evm.finish();

        let assembled_system_txs = self.shared_ctx.inner.borrow().assembled_system_txs.clone();
        // merge all transitions into bundle state
        db.merge_transitions(BundleRetention::Reverts);

        // calculate the state root
        let state_root_start = std::time::Instant::now();
        let hashed_state = state.hashed_post_state(&db.bundle_state);
        let parent_hash = self.parent.hash_slow();
        
        let (state_root, trie_updates) = match STATE_ROOT_ALGORITHM {
            "sparse" => {
                tracing::debug!("use sparse state root calculation");
                let payload_handle = self.payload_handle.as_mut().unwrap();
                // TODO: uncomment this when we have a way to stop the prewarming execution.
                // payload_handle.stop_prewarming_execution();

                // TODO: submit the final state to the state hook again.
                // let mut state_hook = payload_handle.state_hook();
                // let source_index = result.receipts.len();
                // // Note: OnStateHook 期望的是 EvmState(HashMap<Address, Account>)，此处仅做空回补以满足接口
                // let empty_evm_state: std::collections::HashMap<alloy_primitives::Address, revm::revm_state::Account> = Default::default();
                // state_hook.on_state(StateChangeSource::Transaction(source_index), &empty_evm_state);
                match payload_handle.state_root() {
                    Ok(StateRootComputeOutcome { state_root, trie_updates }) => {
                        (state_root, trie_updates)
                    }
                    Err(error) => {
                        return Err(BlockExecutionError::other(error));
                    }
                }
            }
            "parallel" => {
                tracing::debug!("use parallel state root calculation");
                let engine_api_tx = match crate::shared::get_engine_api_tx() {
                    Some(tx) => tx,
                    None => {
                        return Err(BlockExecutionError::other(
                            std::io::Error::new(std::io::ErrorKind::Other, "engine api not found"),
                        ))
                    }
                };
                let mut parallel_state_root_task = if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let fut = async {
                        request_parallel_state_root(&engine_api_tx, parent_hash).await
                    };
                    tokio::task::block_in_place(|| handle.block_on(fut))
                } else {
                    let fut = async {
                        request_parallel_state_root(&engine_api_tx, parent_hash).await
                    };
                    match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                        Ok(rt) => {
                            rt.block_on(fut)
                        }
                        Err(err) => {
                            Err(BSCEngineMessageError::internal(err))
                        }
                    }
                }.map_err(BlockExecutionError::other)?;
                parallel_state_root_task.append_state(&hashed_state);
                parallel_state_root_task
                    .incremental_root_with_updates()
                    .map_err(BlockExecutionError::other)?
            }
            _ => {
                tracing::debug!("use serial state root calculation");
                state
                    .state_root_with_updates(hashed_state.clone())
                    .map_err(BlockExecutionError::other)?
            }
        };

        let state_root_duration = state_root_start.elapsed();

        let user_tx_len = self.transactions.len();
        let system_tx_len = assembled_system_txs.len();
        self.transactions.extend(assembled_system_txs);
        let total_tx_len = self.transactions.len();

        let (transactions, senders): (Vec<_>, Vec<_>) =
            self.transactions.into_iter().map(|tx| tx.into_parts()).unzip();

        // BlockAssemblerInput is non_exhaustive. 
        // So define a new struct BscBlockAssemblerInput and a new interface assemble_block_bsc.
        let bsc_input: BscBlockAssemblerInput<'_, '_, BscBlockExecutorFactory> = BscBlockAssemblerInput {
            evm_env,
            execution_ctx: self.ctx,
            parent: self.parent,
            transactions: transactions.clone(),
            output: &result,
            bundle_state: &db.bundle_state,
            state_provider: &state,
            state_root,
        };
        let assemble_start = std::time::Instant::now();
        let block = self.assembler.assemble_block_bsc(bsc_input)?;

        // cache current validators and turn length
        let current_validators = self.shared_ctx.inner.borrow().current_validators.clone();
        if let Some((validators, vote_addresses)) = current_validators {
            VALIDATOR_CACHE.lock().unwrap().insert(block.header.hash_slow(), (validators, vote_addresses));
            tracing::debug!("Succeed to update validator cache in builder, block_number: {}, block_hash: {}", block.header.number, block.header.hash_slow());
        }
        if let Some(turn_length) = self.shared_ctx.inner.borrow().turn_length {
            TURN_LENGTH_CACHE.lock().unwrap().insert(block.header.hash_slow(), turn_length);
            tracing::debug!("Succeed to update turn length cache in builder, block_number: {}, block_hash: {}", block.header.number, block.header.hash_slow());
        }
        let assemble_duration = assemble_start.elapsed();
        
        let finish_duration = finish_start.elapsed();
        tracing::debug!(
            target: "bsc::builder",
            block_number = %block.header.number,
            block_hash = %block.header.hash_slow(),
            user_tx_len = user_tx_len,
            system_tx_len = system_tx_len,
            total_tx_len = total_tx_len,
            finish_duration_ms = finish_duration.as_millis(),
            state_root_duration_ms = state_root_duration.as_millis(),
            assemble_duration_ms = assemble_duration.as_millis(),
            "Succeed to seal block"
        );

        let block = RecoveredBlock::new_unhashed(block, senders);
        Ok(BlockBuilderOutcome { execution_result: result, hashed_state, trie_updates, block })
    }

    fn executor_mut(&mut self) -> &mut Self::Executor {
        &mut self.executor
    }

    fn executor(&self) -> &Self::Executor {
        &self.executor
    }

    fn into_executor(self) -> Self::Executor {
        self.executor
    }
}

type BscProviderFactory = reth_provider::providers::BlockchainProvider<
    reth::api::NodeTypesWithDBAdapter<crate::node::BscNode, std::sync::Arc<reth_db::DatabaseEnv>>
>;

pub async fn request_parallel_state_root(
    engine_api_tx: &EngineApiTx<NodeAdapter<BscNode>>,
    parent_hash: BlockHash,
) -> Result<ParallelStateRoot<BscProviderFactory>, BSCEngineMessageError> {
    let (tx, rx) = oneshot::channel();
    let _ = engine_api_tx.send(EngineApiRequest::Custom(
        CustomRequestMessage::RequestParallelStateRoot { parent_hash, tx }
    ));
    rx.await.map_err(BSCEngineMessageError::internal)?.map_err(BSCEngineMessageError::internal)
}

pub async fn request_payload_processor(
    engine_api_tx: &EngineApiTx<NodeAdapter<BscNode>>,
) -> Result<PayloadProcessor<crate::node::evm::config::BscEvmConfig>, BSCEngineMessageError> {
    let (tx, rx) = oneshot::channel();
    let _ = engine_api_tx.send(EngineApiRequest::Custom(
        CustomRequestMessage::RequestPayloadProcessor { tx }
    ));
    rx.await.map_err(BSCEngineMessageError::internal)?.map_err(BSCEngineMessageError::internal)
}
pub async fn request_parallel_ctx(
    engine_api_tx: &EngineApiTx<NodeAdapter<BscNode>>,
    parent_hash: BlockHash,
    allocated_trie_input: Option<TrieInput>,
) -> Result<CustomParallelCtx<BscProviderFactory, BscPrimitives>, BSCEngineMessageError> {
    let (tx, rx) = oneshot::channel();
    let _ = engine_api_tx.send(EngineApiRequest::Custom(
        CustomRequestMessage::RequestParallelCtx { parent_hash, allocated_trie_input, tx }
    ));
    rx.await.map_err(BSCEngineMessageError::internal)?.map_err(BSCEngineMessageError::internal)
}

