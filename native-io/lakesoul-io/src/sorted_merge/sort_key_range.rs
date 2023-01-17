use std::sync::Arc;
use std::cmp::Ordering;


use arrow::record_batch::RecordBatch;
use arrow::row::{Row, Rows};

// A range in one record batch with same primary key
pub struct SortKeyRangeInBatch {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // not included
    pub(crate) stream_idx: usize,
    pub(crate) batch: Arc<RecordBatch>,
    pub(crate) rows: Arc<Rows>,
}

impl SortKeyRangeInBatch {
    pub fn new(begin_row: usize, end_row: usize, stream_idx: usize, batch: Arc<RecordBatch>, rows: Arc<Rows>) -> Self {
        SortKeyRangeInBatch {
            begin_row,
            end_row,
            stream_idx,
            batch,
            rows,
        }
    }

    pub(crate) fn current(&self) -> Row<'_> {
        self.rows.row(self.begin_row)
    }

    #[inline(always)]
    /// Return the stream index of this range
    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }


    #[inline(always)]
    /// Return true if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.begin_row >= self.rows.num_rows()
    }


    #[inline(always)]
    /// Returns the cursor's current row, and advances the cursor to the next row
    pub fn advance(&mut self) -> SortKeyRangeInBatch {
        // assert!(!self.is_finished());
        // let t = self.cur_row;
        // self.cur_row += 1;
        // t
        let current = self.clone();
        self.begin_row = self.end_row;
        while self.end_row < self.rows.num_rows() {
            // check if next row in this batch has same sort key
            if self.rows.row(self.end_row) == self.rows.row(self.begin_row) {
                self.end_row = self.end_row + 1;
            }
        }
        current
    }

}

impl Clone for SortKeyRangeInBatch {
    fn clone(&self) -> Self {
        SortKeyRangeInBatch::new(self.begin_row, self.end_row, self.stream_idx, self.batch.clone(), self.rows.clone())
    }
}

impl PartialEq for SortKeyRangeInBatch {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for SortKeyRangeInBatch {}

impl PartialOrd for SortKeyRangeInBatch {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKeyRangeInBatch {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current()
            .cmp(&other.current())
            .then_with(|| self.stream_idx.cmp(&other.stream_idx))
    }
}