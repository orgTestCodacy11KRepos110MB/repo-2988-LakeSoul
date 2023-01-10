use crate::sorted_merge::sorted_stream_merger::SortKeyRange;

use futures::stream::{Fuse, FusedStream};


use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use datafusion::error::Result;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::SendableRecordBatchStream;
use smallvec::SmallVec;

pub enum SortedFileType {
    UniqueSortedPrimaryKeys,
    NonUniqueSortedPrimaryKeys,
}

pub enum MergingType {
    SingledFileMerge,
    MultipleFileWithSameSchemaMerge,
    MultipleFileWithDifferentSchemaMerge,
}

pub enum MergingLogicType {
    UseLast,
    UseAssociativeExpr,
    UseJavaMergeOp,
}


#[async_trait]
pub trait StreamSortKeyRangeFetcher {
    fn new(
        stream_idx: usize,
        stream: Fuse<SendableRecordBatchStream>,
        expressions: &[PhysicalSortExpr],
        schema: SchemaRef,
    ) -> Result<Self>
    where
        Self: Sized;

    async fn init_batch(&mut self) -> Result<()>;

    fn is_terminated(&self) -> bool;

    async fn fetch_sort_key_range_from_stream(
        &mut self,
        current_range: Option<&SortKeyRange>,
    ) -> Result<Option<SortKeyRange>>;
}

#[async_trait]
pub trait StreamSortKeyRangeCombiner {
    type Fetcher: StreamSortKeyRangeFetcher;

    fn with_fetchers(fetchers: Vec<Self::Fetcher>) -> Self;

    fn fetcher_num(self) -> usize;

    async fn init(&mut self) -> Result<()>;

    async fn next(&mut self) -> Result<Option<SmallVec<[SortKeyRange; 4]>>>;
}
