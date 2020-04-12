package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The current record on the right page [ADDED]
        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            if (!leftIterator.hasNext()) { //No more pages in the left relation with records.
                this.leftRecordIterator = null;
                this.leftRecord = null;
                return;
            }
            int blocksize = numBuffers - 2;
            //return iterator over B-2 pages in left table
            this.leftRecordIterator = getBlockIterator(getLeftTableName(), leftIterator, blocksize);
            this.leftRecord = leftRecordIterator.next();
            leftRecordIterator.markPrev();
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it. Also sets right record to first record on right page, and MARKS record iterator.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            this.rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);
            if (rightRecordIterator.hasNext()) {
                this.rightRecord = rightRecordIterator.next();
                rightRecordIterator.markPrev();
            } else {
                this.rightRecord = null;
            }
        }

        /**
         * Reset left page iterator to previously marked spot, and updates left record if it can.
         */
        private void resetLeft() {
            leftRecordIterator.reset();
            if (leftRecordIterator.hasNext()) {
                leftRecord = leftRecordIterator.next();
            } else {
                leftRecord = null;
            }
            leftRecordIterator.markPrev();
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            //GAME PLAN: Iterate through both relations until match, then set iterators to correct place.
            if (leftRecord == null) throw new NoSuchElementException();
            nextRecord = null; //Reset nextRecord container space.
            while (!hasNext()) {
                boolean nextLRec = leftRecordIterator.hasNext(); //has next Left table record
                boolean nextRRec = rightRecordIterator.hasNext(); //has next right table record
                boolean nextLBlock = leftIterator.hasNext(); //has next Left table BLOCK (B-2 pages)
                boolean nextRPage = rightIterator.hasNext(); //has next right table PAGE
                //CASE 1: Both relations in middle of page(s), records left to check.
                if (rightRecord != null) {
                    matchUp(); //Try to find match.
                }
                //CASE 2: no more records or pages in either (searched every possible).
                //I.e. no more records to return
                else if (!nextLRec && !nextRRec && !nextRPage && !nextLBlock) {
                    throw new NoSuchElementException();
                }
                //CASE 3: New page on the right to check through.
                else if (!nextLRec && !nextRRec && nextRPage) {
                    resetLeft();
                    fetchNextRightPage();
                }
                //CASE 4: End of left page (another page), Iterated through all possible right records.
                else if (!nextLRec && !nextRRec && !nextRPage && nextLBlock) {
                    fetchNextLeftBlock();
                    //RESET right iterator to beginning of RIGHT table.
                    rightIterator = getPageIterator(getRightTableName());
                    fetchNextRightPage();
                }
                //CASE 5: In middle of left page and end of right records.
                else {
                    leftRecord = leftRecordIterator.next();
                    rightRecordIterator.reset();
                    rightRecord = rightRecordIterator.next();
                    rightRecordIterator.markPrev();
                }
            }
        }

        /** Search through both relations for a match, assuming records left.**/
        private void matchUp() {
            DataBox ri = leftRecord.getValues().get(getLeftColumnIndex());
            DataBox sj = rightRecord.getValues().get(getRightColumnIndex());
            if (match(ri, sj)) {
                yieldMatch(); //Yield record match
            }
            if (rightRecordIterator.hasNext()) {
                rightRecord = rightRecordIterator.next();
            } else {
                rightRecord = null;
            }
        }

        /**
         * Helper function that yields the combined record < ri, sj >,
         * which we know through state attributes leftRecord and rightRecord.
         */
        private void yieldMatch() {
            List<DataBox> ri_rec = new ArrayList<>(leftRecord.getValues());
            List<DataBox> sj_rec = new ArrayList<>(rightRecord.getValues());
            ri_rec.addAll(sj_rec);
            this.nextRecord = new Record(ri_rec); //YIELD <ri, sj> record.
        }

        /**
         * Helper function to check for column value matches.
         * @param ri Left relation val
         * @param sj Right relation val
         * @return if match
         */
        private boolean match(DataBox ri, DataBox sj) {
            return ri.equals(sj);
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
