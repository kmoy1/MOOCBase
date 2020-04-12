package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;

import java.lang.reflect.Array;
import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        Iterator<Record> iter = run.iterator();
        ArrayList<Record> sortedRecords = runThrough(iter);
        Run sorted = createRun();
        sorted.addRecords(sortedRecords);
        return sorted;
    }

    /**
     * Helper method that takes an iterator over a run and returns a sorted records arraylist,
     * using state attribute comparator to sort.
     * @param iter iterator over Run
     * @return sorted arraylist of records
     */
    private ArrayList<Record> runThrough(Iterator<Record> iter) {
        ArrayList<Record> r = new ArrayList<>();
        while (iter.hasNext()) {
            r.add(iter.next());
        }
        r.sort(comparator);
        return r;
    }

    /**
     * Given a list of sorted runs, returns a NEW run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        Run merged = createRun(); //FINAL RETURN RUN (SORTED)
        List<Iterator> iters = getRunIterators(runs); //LIST of iterators over all merging pieces.
        PriorityQueue<Pair<Record, Integer>> pq = queueUp(runs); //priority queue, filled
        while (!pq.isEmpty()) {
            Pair<Record, Integer> p = pq.poll(); //get first run record
            List<DataBox> recordVals = p.getFirst().getValues();
            merged.addRecord(recordVals); //add record values (single record) to merged run.
            Iterator<Record> iter2 = iters.get(p.getSecond()); //iterator on next run.
            if (iter2.hasNext()) {//Go through run and add its (r,i) pairs to the priority queue and repeat merge process.
                Record r = iter2.next();
                Pair<Record, Integer> p2 = new Pair(r, p.getSecond());
                pq.add(p2); //Add.
            }
        }
        return merged;
    }

    /**
     * Helper function to return all iterators over all runs.
     * @param runs runs list
     * @return list of iterators each corresponding to a run in runs
     */
    private List<Iterator> getRunIterators(List<Run> runs) {
        ArrayList<Iterator> its = new ArrayList<>();
        int num_runs = runs.size();
        for (int i = 0; i < num_runs; i ++) {
            Iterator<Record> iter = runs.get(i).iterator();//iterator for run i in runs
            Record nextRec = iter.next();
            Pair<Record, Integer> pair = new Pair(nextRec, i);
            its.add(iter);
        }
        return its;
    }

    /**
     * Helper function to fill up priority queue with min records from each run.
     * @param runs runs list
     * @return filled priority queue
     */
    private PriorityQueue<Pair<Record, Integer>> queueUp(List<Run> runs) {
        PriorityQueue<Pair<Record, Integer>> pq = new PriorityQueue(runs.size(), new RecordPairComparator());
        for (int i = 0; i < runs.size(); i ++) {
            Record r = runs.get(i).iterator().next();
            Pair<Record, Integer> p = new Pair(r, i); //r = smallest record from run i.
            pq.add(p);
        }
        return pq;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time. It is okay for the last sorted run
     * to use less than (numBuffers - 1) input runs if N is not a
     * perfect multiple.
     */
    public List<Run> mergePass(List<Run> runs) {
        ArrayList<Run> mergedSortedRuns = new ArrayList<>();
        int N = runs.size();
        int Bm1 = numBuffers - 1; //B - 1
        for(int i = 0; i < N; i += Bm1) {
            List<Run> sortedRunsList = runs.subList(i, i + Bm1); //sorted runs of B pages.
            mergedSortedRuns.add(mergeSortedRuns(sortedRunsList)); //Add run to full. If multiple, recursively handled in sort() method below.
        }
        return mergedSortedRuns;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        BacktrackingIterator iter_pages = this.transaction.getPageIterator(this.tableName); //iterator over pages in table.
        int B = numBuffers * transaction.getNumEntriesPerPage(tableName); //total # records (might not need to be used)
        List<Run> sortedRuns = new ArrayList<>(); //Return this
        while (iter_pages.hasNext()) {
            //Necessarily advances page iterator as well (learned from BNLJ debugging for like 10 hours)
            BacktrackingIterator<Record> iter_record = transaction.getBlockIterator(tableName, iter_pages, numBuffers);
            Run run = createRunFromIterator(iter_record);
            sortedRuns.add(sortRun(run));
        }
        while (true) { // Pass 1 through n.
            if (sortedRuns.size() <= 1) {
                break;
            }
            sortedRuns = mergePass(sortedRuns);
        }
        return sortedRuns.get(0).tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

