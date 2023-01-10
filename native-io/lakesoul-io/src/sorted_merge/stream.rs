use std::sync::Arc;
use std::fmt::{Debug, Formatter};
use std::task::{Context, Poll};
use std::collections::{BinaryHeap, VecDeque};
use std::cmp::Reverse;
use std::pin::Pin;



use futures::stream::{Fuse, FusedStream};
use futures::{Stream, StreamExt};



use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use arrow::{
    array::{make_array as make_arrow_array, MutableArrayData},
    datatypes::SchemaRef,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};


use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream, RecordBatchStream};

use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;


use datafusion::physical_plan::sorts::{RowIndex, SortKeyCursor};

pub(crate) struct SortedStream {
    stream: SendableRecordBatchStream,
    mem_used: usize,
}

impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSorterStream")
    }
}

impl SortedStream {
    pub(crate) fn new(stream: SendableRecordBatchStream, mem_used: usize) -> Self {
        Self { stream, mem_used }
    }
}

struct MergingStreams {
    /// The sorted input streams to merge together
    streams: Vec<Fuse<SendableRecordBatchStream>>,
    /// number of streams
    num_streams: usize,
}

impl std::fmt::Debug for MergingStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergingStreams")
            .field("num_streams", &self.num_streams)
            .finish()
    }
}

impl MergingStreams {
    fn new(input_streams: Vec<Fuse<SendableRecordBatchStream>>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: input_streams,
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}


#[derive(Debug)]
pub(crate) struct SortPreservingMergeStream {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    streams: MergingStreams,

    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<RecordBatch>>,

    /// Maintain a flag for each stream denoting if the current cursor
    /// has finished and needs to poll from the stream
    cursor_finished: Vec<bool>,

    /// The accumulated row indexes for the next record batch
    in_progress: Vec<RowIndex>,

    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,


    /// If the stream has encountered an error
    aborted: bool,

    /// An id to uniquely identify the input stream batch
    next_batch_id: usize,

    /// Heap that yields [`SortKeyCursor`] in increasing order
    heap: BinaryHeap<Reverse<SortKeyCursor>>,

    /// target batch size
    batch_size: usize,

    /// row converter
    row_converter: RowConverter,
}

impl SortPreservingMergeStream {
    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        batch_size: usize,
    ) -> Result<Self> {
        let stream_count = streams.len();
        let batches = (0..stream_count)
            .into_iter()
            .map(|_| VecDeque::new())
            .collect();
        let wrappers = streams.into_iter().map(|s| s.stream.fuse()).collect();

        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(&schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;
        let row_converter = RowConverter::new(sort_fields);

        Ok(Self {
            schema,
            batches,
            cursor_finished: vec![true; stream_count],
            streams: MergingStreams::new(wrappers),
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            aborted: false,
            in_progress: vec![],
            next_batch_id: 0,
            heap: BinaryHeap::with_capacity(stream_count),
            batch_size,
            row_converter,
        })
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<ArrowResult<()>> {
        if !self.cursor_finished[idx] {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }
        let mut empty_batch = false;
        {
            let stream = &mut self.streams.streams[idx];
            if stream.is_terminated() {
                return Poll::Ready(Ok(()));
            }

            // Fetch a new input record and create a cursor from it
            match futures::ready!(stream.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(())),
                Some(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let cols = self
                            .column_expressions
                            .iter()
                            .map(|expr| {
                                Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let rows = match self.row_converter.convert_columns(&cols) {
                            Ok(rows) => rows,
                            Err(e) => {
                                return Poll::Ready(Err(ArrowError::ExternalError(
                                    Box::new(e),
                                )));
                            }
                        };

                        let cursor = SortKeyCursor::new(
                            idx,
                            self.next_batch_id, // assign this batch an ID
                            rows,
                        );
                        self.next_batch_id += 1;
                        self.heap.push(Reverse(cursor));
                        self.cursor_finished[idx] = false;
                        self.batches[idx].push_back(batch)
                    } else {
                        empty_batch = true;
                    }
                }
            }
        }

        if empty_batch {
            self.maybe_poll_stream(cx, idx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        // Mapping from stream index to the index of the first buffer from that stream
        let mut buffer_idx = 0;
        let mut stream_to_buffer_idx = Vec::with_capacity(self.batches.len());

        for batches in &self.batches {
            stream_to_buffer_idx.push(buffer_idx);
            buffer_idx += batches.len();
        }

        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let arrays = self
                    .batches
                    .iter()
                    .flat_map(|batch| {
                        batch.iter().map(|batch| batch.column(column_idx).data())
                    })
                    .collect();

                let mut array_data = MutableArrayData::new(
                    arrays,
                    field.is_nullable(),
                    self.in_progress.len(),
                );

                if self.in_progress.is_empty() {
                    return make_arrow_array(array_data.freeze());
                }

                let first = &self.in_progress[0];
                let mut buffer_idx =
                    stream_to_buffer_idx[first.stream_idx] + first.batch_idx;
                let mut start_row_idx = first.row_idx;
                let mut end_row_idx = start_row_idx + 1;

                for row_index in self.in_progress.iter().skip(1) {
                    let next_buffer_idx =
                        stream_to_buffer_idx[row_index.stream_idx] + row_index.batch_idx;

                    if next_buffer_idx == buffer_idx && row_index.row_idx == end_row_idx {
                        // subsequent row in same batch
                        end_row_idx += 1;
                        continue;
                    }

                    // emit current batch of rows for current buffer
                    array_data.extend(buffer_idx, start_row_idx, end_row_idx);

                    // start new batch of rows
                    buffer_idx = next_buffer_idx;
                    start_row_idx = row_index.row_idx;
                    end_row_idx = start_row_idx + 1;
                }

                // emit final batch of rows
                array_data.extend(buffer_idx, start_row_idx, end_row_idx);
                make_arrow_array(array_data.freeze())
            })
            .collect();

        self.in_progress.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // Additionally as `in_progress` has been drained, there are no longer
        // any RowIndex's reliant on the batch indexes
        //
        // We can therefore drop all but the last batch for each stream
        for batches in &mut self.batches {
            if batches.len() > 1 {
                // Drain all but the last batch
                batches.drain(0..(batches.len() - 1));
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

impl Stream for SortPreservingMergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

impl SortPreservingMergeStream {
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted streams have a cursor from which
        // rows can be pulled
        for i in 0..self.streams.num_streams() {
            match futures::ready!(self.maybe_poll_stream(cx, i)) {
                Ok(_) => {}
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // NB timer records time taken on drop, so there are no
        // calls to `timer.done()` below.
        

        loop {
            match self.heap.pop() {
                Some(Reverse(mut cursor)) => {
                    let stream_idx = cursor.stream_idx();
                    let batch_idx = self.batches[stream_idx].len() - 1;
                    let row_idx = cursor.advance();

                    let mut cursor_finished = false;
                    // insert the cursor back to heap if the record batch is not exhausted
                    if !cursor.is_finished() {
                        self.heap.push(Reverse(cursor));
                    } else {
                        cursor_finished = true;
                        self.cursor_finished[stream_idx] = true;
                    }

                    self.in_progress.push(RowIndex {
                        stream_idx,
                        batch_idx,
                        row_idx,
                    });

                    if self.in_progress.len() == self.batch_size {
                        return Poll::Ready(Some(self.build_record_batch()));
                    }

                    // If removed the last row from the cursor, need to fetch a new record
                    // batch if possible, before looping round again
                    if cursor_finished {
                        match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                            Ok(_) => {}
                            Err(e) => {
                                self.aborted = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
                None if self.in_progress.is_empty() => return Poll::Ready(None),
                None => return Poll::Ready(Some(self.build_record_batch())),
            }
        }
    }
}

impl RecordBatchStream for SortPreservingMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}





#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use arrow::array::ArrayRef;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::FutureExt;
    use tokio_stream::StreamExt;

    use datafusion::arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use datafusion::from_slice::FromSlice;
    use datafusion::execution::context::TaskContext;
    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{collect, common};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::metrics::{MemTrackingMetrics, ExecutionPlanMetricsSet};


    use datafusion::{assert_batches_eq, test_util};

    use super::*;

    #[tokio::test]
    async fn test_merge_interleave() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("b"),
            Some("d"),
            Some("f"),
            Some("h"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 10 | b | 1970-01-01 00:00:00.000000004 |",
                "| 2  | c | 1970-01-01 00:00:00.000000007 |",
                "| 20 | d | 1970-01-01 00:00:00.000000006 |",
                "| 7  | e | 1970-01-01 00:00:00.000000006 |",
                "| 70 | f | 1970-01-01 00:00:00.000000002 |",
                "| 9  | g | 1970-01-01 00:00:00.000000005 |",
                "| 90 | h | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |", // input b2 before b1
                "| 3  | j | 1970-01-01 00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_some_overlap() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[70, 90, 30, 100, 110]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 70  | c | 1970-01-01 00:00:00.000000004 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 90  | d | 1970-01-01 00:00:00.000000006 |",
                "| 30  | e | 1970-01-01 00:00:00.000000002 |",
                "| 3   | e | 1970-01-01 00:00:00.000000008 |",
                "| 100 | f | 1970-01-01 00:00:00.000000002 |",
                "| 110 | g | 1970-01-01 00:00:00.000000006 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_no_overlap() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 2  | b | 1970-01-01 00:00:00.000000007 |",
                "| 7  | c | 1970-01-01 00:00:00.000000006 |",
                "| 9  | d | 1970-01-01 00:00:00.000000005 |",
                "| 3  | e | 1970-01-01 00:00:00.000000008 |",
                "| 10 | f | 1970-01-01 00:00:00.000000004 |",
                "| 20 | g | 1970-01-01 00:00:00.000000006 |",
                "| 70 | h | 1970-01-01 00:00:00.000000002 |",
                "| 90 | i | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_three_partitions() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20, 20, 60]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[100, 200, 700, 900, 300]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2], vec![b3]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 10  | e | 1970-01-01 00:00:00.000000040 |",
                "| 100 | f | 1970-01-01 00:00:00.000000004 |",
                "| 3   | f | 1970-01-01 00:00:00.000000008 |",
                "| 200 | g | 1970-01-01 00:00:00.000000006 |",
                "| 20  | g | 1970-01-01 00:00:00.000000060 |",
                "| 700 | h | 1970-01-01 00:00:00.000000002 |",
                "| 70  | h | 1970-01-01 00:00:00.000000020 |",
                "| 900 | i | 1970-01-01 00:00:00.000000002 |",
                "| 90  | i | 1970-01-01 00:00:00.000000020 |",
                "| 300 | j | 1970-01-01 00:00:00.000000006 |",
                "| 30  | j | 1970-01-01 00:00:00.000000060 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    async fn _test_merge(
        partitions: &[Vec<RecordBatch>],
        exp: &[&str],
        context: Arc<TaskContext>,
    ) {
        let schema = partitions[0][0].schema();
        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: Default::default(),
            },
        ];
        let exec = MemoryExec::try_new(partitions, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, context).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }



    #[tokio::test]
    async fn test_async() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let files:Vec<String> = vec![];
        let mut streams = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            let mut stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            streams.push(SortedStream::new(
                stream,
                0,
            ));
        }

        let schema = test_util::aggr_test_schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("c12", &schema).unwrap(),
            options: SortOptions::default(),
        }];


        let merge_stream = SortPreservingMergeStream::new_from_streams(
            streams,
            schema,
            sort.as_slice(),
            task_ctx.session_config().batch_size(),
        )
        .unwrap();
        let mut merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_eq!(merged.len(), 1);
        let merged = merged.remove(0);



        let partition = arrow::util::pretty::pretty_format_batches(&[merged])
            .unwrap()
            .to_string();

    }


}
