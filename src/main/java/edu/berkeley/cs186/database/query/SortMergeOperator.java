package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import javax.xml.crypto.Data;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private String ltName; //Left table name
        private String rtName; //right table name
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private SortOperator sl; //sorting on left table
        private SortOperator sr; //sorting on right table
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private LeftRecordComparator LRComp = new LeftRecordComparator();
        private RightRecordComparator RRComp = new RightRecordComparator();

        private SortMergeIterator() {
            super();
            this.ltName = getLeftTableName(); //unsorted
            this.rtName = getRightTableName();
            SortOperator sleft = new SortOperator(getTransaction(), ltName, new LeftRecordComparator());
            this.leftIterator = getTransaction().getRecordIterator(sleft.sort()); // iterates over sorted left table.
            SortOperator sright = new SortOperator(getTransaction(), rtName, new RightRecordComparator());
            this.rightIterator = getTransaction().getRecordIterator(sright.sort()); //iterates over sorted right table.
            this.nextRecord = null;
            advanceLeft();//Get next left row
            advanceRight(); //Get next right row
            marked = false;
            rightIterator.markPrev();
            //SAME as BNLJ
            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                nextRecord = null;
            }
        }

        /**
         * Advance left iterator, and update left record
         */
        private void advanceLeft() {
            if (leftIterator.hasNext()) {
                leftRecord = leftIterator.next();
            } else {
                leftRecord = null;
            }
        }

        /**
         * Advance right iterator, and update right record
         */
        private void advanceRight() {
            if (rightIterator.hasNext()) {
                rightRecord = rightIterator.next();
            } else {
                rightRecord = null;
            }
        }

        /**
         * Reset right iterator to previously marked spot and set rightrecord.
         */
        private void resetRight() {
            rightIterator.reset();
            if (rightIterator.hasNext()) {
                rightRecord = rightIterator.next();
            } else{
                rightRecord = null;
            }
        }
        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
//            fetchNextRecord();
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record record = this.nextRecord;
            try {
                fetchNextRecord(); //Update nextRecord at the same time.
            } catch (NoSuchElementException e) { //No records left
                this.nextRecord = null;
            }
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            if (leftRecord == null) throw new NoSuchElementException();
            nextRecord = null; //Clear space for next record.
            do {
                //CASE 1: NOT marked and right table not at end (of page)
                if (!marked && rightRecord != null) {
                    findMatch();//Advances both tables iteratively based on comparison flag value, until match.
                    rightIterator.markPrev();
                    marked = true; //marked a matched row.
                }
                //MATCHED (follows from findMatch()
                if (rightRecord != null && LRComp.compare(leftRecord,rightRecord) == 0) {
                    yieldMatch();
                    advanceRight();
                } else { //No match. Advance to next row in left iterator, and reset right.
                    resetRight();
                    advanceLeft();
                    if (leftRecord == null) {
                        return;
                    }
                    marked = false;
                }
            } while (!hasNext());
        }

        /**
         * Helper function that will iteratively call advance our left and
         * right iterators until a match is found:
         * when compare(leftrecord, rightrecord) = 0.
         */
        private void findMatch() {
            while (LRComp.compare(leftRecord, rightRecord) < 0) {
                advanceLeft();
                if (leftRecord == null) {
                    return;
                }
            }
            while (LRComp.compare(leftRecord, rightRecord) > 0) {
                advanceRight();
            }
        }

        /**
         * Helper function that yields the combined record < ri, sj >,
         * which we know through state attributes leftRecord and rightRecord,
         * and assigns it to nextrecord.
         */
        private void yieldMatch() {
            List<DataBox> ri_rec = new ArrayList<>(leftRecord.getValues());
            List<DataBox> sj_rec = new ArrayList<>(rightRecord.getValues());
            ri_rec.addAll(sj_rec);
            this.nextRecord = new Record(ri_rec); //YIELD <ri, sj> record.
        }



        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
