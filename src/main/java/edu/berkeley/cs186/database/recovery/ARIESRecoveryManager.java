package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.Transaction.Status;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // A commit record should be emitted
        TransactionTableEntry tableEntry = transactionTable.get(transNum);
        LogRecord commitRecord = new CommitTransactionLogRecord(transNum, tableEntry.lastLSN);
        long newLSN = logManager.appendToLog(commitRecord);

        // the log should be flushed
        pageFlushHook(newLSN);

        // the transaction table and the transaction status should be updated.
        tableEntry.lastLSN = newLSN;
        tableEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        // return LSN of the commit record
        return newLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        // An abort record should be emitted
        TransactionTableEntry tableEntry = transactionTable.get(transNum);
        LogRecord abortRecord = new AbortTransactionLogRecord(transNum, tableEntry.lastLSN);
        logManager.appendToLog(abortRecord);

        long newLSN = abortRecord.LSN;

        // the transaction table and the transaction status should be updated.
        tableEntry.lastLSN = newLSN;
        tableEntry.transaction.setStatus(Transaction.Status.ABORTING);

        // return LSN of the abort record
        return newLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry tableEntry = transactionTable.get(transNum);
        Transaction transaction = tableEntry.transaction;

        // any changes that need to be undone should be undone
        if (transaction.getStatus() == Transaction.Status.ABORTING) {

            // starting from the last log entry by this transaction
            long undoLSN = tableEntry.lastLSN;
            LogRecord undoRecord = logManager.fetchLogRecord(undoLSN);
            long undoRecordPrevLSN = 0L;

            // until the beginning of the transaction
            while (undoRecord.LSN != 0L) {

                Optional<Long> undoLSNOptional = undoRecord.getPrevLSN();
                /* The undoRecord's prevLSN, or 0 when there are no more entries */
                undoRecordPrevLSN = undoLSNOptional.orElse(0L);

                // undo the log entry and emit a CLR
                if (undoRecord.isUndoable()) {

                    Pair<LogRecord, Boolean> undoPair = undoRecord.undo(undoRecordPrevLSN);

                    /* the CLR corresponding to this log record */
                    LogRecord addedRecord = undoPair.getFirst();

                    /* a boolean that is true if the log must be flushed up to the CLR after executing the undo, and false otherwise. */
                    boolean flushLog = undoPair.getSecond();

                    // emit CLRs
                    logManager.appendToLog(addedRecord);
                    tableEntry.lastLSN = addedRecord.LSN;

                    if (flushLog) pageFlushHook(addedRecord.LSN);

                }

                // then iterate back to that entry's previous LSN
                undoRecord = logManager.fetchLogRecord(undoRecordPrevLSN);
            }
        }

        // the transaction should be removed from the transaction table.
        transactionTable.remove(transNum);

        // the end record should be emitted
        LogRecord endRecord = new EndTransactionLogRecord(transNum, tableEntry.lastLSN);
        logManager.appendToLog(endRecord);
        long newLSN = endRecord.LSN;

        // the transaction status should be updated.
        transaction.setStatus(Transaction.Status.COMPLETE);

        // return LSN of the end record
        return newLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        UpdatePageLogRecord updateLog;

        // TODO(proj5): implement
        if (before.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {

            UpdatePageLogRecord undoLog = new UpdatePageLogRecord(
                    transNum, pageNum, transactionTable.get(transNum).lastLSN,
                    pageOffset, before, null);

            logManager.appendToLog(undoLog);

            updateLog = new UpdatePageLogRecord(
                    transNum, pageNum, undoLog.LSN,
                    pageOffset, null, after);

        }
        else {
            updateLog = new UpdatePageLogRecord(
                    transNum, pageNum, transactionTable.get(transNum).lastLSN,
                    pageOffset, before, after);
        }

        long LSN = logManager.appendToLog(updateLog);

        // the transaction table should be updated accordingly.
        TransactionTableEntry tableEntry = transactionTable.get(transNum);
        tableEntry.lastLSN = LSN;
        tableEntry.touchedPages.add(pageNum);

        dirtyPageTable.putIfAbsent(pageNum, LSN);

        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);
        // if it's aborting, roll back changes
        long lastLSN = transactionEntry.lastLSN; //Will update as we add more (CLR) log records.
        long currentLSN = transactionEntry.lastLSN; //Current LSN of log record to undo. This STARTS at the last LSN of the Xact!

        while (currentLSN > savepointLSN) {
            //First, point to record to UNDO.
            LogRecord currentRecord = logManager.fetchLogRecord(currentLSN);
            //If our record is undoable, produce appropriate CLR and undo.
            if (currentRecord.isUndoable()) {
                Pair<LogRecord, Boolean> CLR = currentRecord.undo(lastLSN);
                LogRecord CLRRecord = CLR.getFirst();
                boolean flush = CLR.getSecond();
                lastLSN = logManager.appendToLog(CLRRecord); //LastLSN updates based on new CLR records inserted.
                if (flush) pageFlushHook(CLRRecord.LSN);
                CLRRecord.redo(diskSpaceManager, bufferManager);

                // UPDATE DPT if we undid a page-related LR.
                if (pageRelated(CLRRecord)) {
                    long pageID = CLRRecord.getPageNum().get();
                    // If CLR record is an UPDATE to undo
                    if (CLRRecord.type == LogType.UNDO_UPDATE_PAGE ||
                            CLRRecord.type == LogType.UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(pageID, lastLSN);
                    }
                    //IF CLR record undid an update that first dirtied page, remove that page from DPT.
                    else if (CLRRecord.type == LogType.UNDO_ALLOC_PAGE) {
                        dirtyPageTable.remove(pageID);
                    }
                }
            }
            //Jump to next log record LSN to undo.
            if (currentRecord.getUndoNextLSN().isPresent()) {
                currentLSN = currentRecord.getUndoNextLSN().get();
            }
            else if (currentRecord.getPrevLSN().isPresent()) {
                currentLSN = currentRecord.getPrevLSN().get();
            }
            else {
                currentLSN = savepointLSN;
            }
        }
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (Long dirtyPage : dirtyPageTable.keySet()) {
            dpt.put(dirtyPage, dirtyPageTable.get(dirtyPage));
        }

        for (Long txnNumber : transactionTable.keySet()) {
            TransactionTableEntry t = transactionTable.get(txnNumber);
            txnTable.put(txnNumber, new Pair<>(t.transaction.getStatus(), t.lastLSN));

            ArrayList<Long> touchedPagesList = new ArrayList<>(t.touchedPages);
            touchedPages.put(txnNumber, touchedPagesList);
        }

        //new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        // debugging note from Nick: end of TODO


        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    private void undoBackTo(long transNum, long lsn) {

        TransactionTableEntry tableEntry = transactionTable.get(transNum);
        Transaction transaction = tableEntry.transaction;

        // starting from the last log entry by this transaction
        long undoLSN = tableEntry.lastLSN;
        LogRecord undoRecord = logManager.fetchLogRecord(undoLSN);
        long undoRecordPrevLSN = 0L;

        // until we reach a record with LSN (or less, which shouldn't happen)
        while (undoRecord.LSN > lsn) {

            Optional<Long> undoLSNOptional = undoRecord.getPrevLSN();
            /* The undoRecord's prevLSN, or 0 when there are no more entries */
            undoRecordPrevLSN = undoLSNOptional.orElse(0L);

            // undo the log entry and emit a CLR
            if (undoRecord.isUndoable()) {

                Pair<LogRecord, Boolean> undoPair = undoRecord.undo(undoRecordPrevLSN);

                /* the CLR corresponding to this log record */
                LogRecord addedRecord = undoPair.getFirst();

                /* a boolean that is true if the log must be flushed up to the CLR after executing the undo, and false otherwise. */
                boolean flushLog = undoPair.getSecond();

                // emit CLRs
                logManager.appendToLog(addedRecord);
                tableEntry.lastLSN = addedRecord.LSN;

                if (flushLog) pageFlushHook(addedRecord.LSN);

            }
            // then iterate back to that entry's previous LSN
            undoRecord = logManager.fetchLogRecord(undoRecordPrevLSN);
        }
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): DONE
        restartAnalysis();
        restartRedo();
        cleanDPT();
        return () -> {
            restartUndo();
            checkpoint();
        };
    }

    /** Get page LSN (LSN of LAST MODIFYING OP on page) given page ID. **/
    private long getPageLSN(long pageID) {
        LockContext pageParent = getPageLockContext(pageID).parentContext();
        long pageLSN = bufferManager.fetchPage(pageParent, pageID, false).getPageLSN();
        return pageLSN;
    }

    /**Helper method that cleans dirty pages from DPT of non dirty pages, i.e.
     * if their page LSN >= flushed LSN, since these pages have a log that hasn't been flushed yet. **/
    private void cleanDPT() {
        Map<Long, Long> DPTCopy = new ConcurrentHashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty && DPTCopy.containsKey(pageNum)) {
                //DPT should ONLY contain dirty pages in the buffer pool.
                dirtyPageTable.put(pageNum, DPTCopy.get(pageNum));
            }
        });
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord currentRecord = iter.next();
            boolean isTransOp = currentRecord.getTransNum().isPresent();
            //IF our log record logged a Xact operation:
            if (isTransOp) {
                long XID = currentRecord.getTransNum().get();
                //Add transaction to Xact table if necessary.
                if (!transactionTable.containsKey(XID)) {
                    transactionTable.put(XID, new TransactionTableEntry(newTransaction.apply(XID)));
                }
                TransactionTableEntry XactTableEntry = transactionTable.get(XID);
                XactTableEntry.lastLSN = currentRecord.getLSN();
                boolean pageRelatedOp = currentRecord.getPageNum().isPresent();
                //If log record logged page-related op.
                if (pageRelatedOp) {
                    long pageID = currentRecord.getPageNum().get();
                    //Add to touchedPages
                    XactTableEntry.touchedPages.add(pageID);
                    //Acquire X lock on page.
                    acquireTransactionLock(XactTableEntry.transaction, getPageLockContext(pageID), LockType.X);
                    //Update DPT.
                    if (!dirtyPageTable.containsKey(pageID)) {
                        dirtyPageTable.put(pageID, currentRecord.getLSN());
                    }
                    if (flushesToDisk(currentRecord)) {
                        dirtyPageTable.remove(pageID);
                    }
                }
                //Update Xact status for Commit/Abort log records.
                if (currentRecord.type == LogType.COMMIT_TRANSACTION) {
                    XactTableEntry.transaction.setStatus(Status.COMMITTING);
                }
                else if (currentRecord.type == LogType.ABORT_TRANSACTION) {
                    XactTableEntry.transaction.setStatus(Status.RECOVERY_ABORTING);
                }
                //Finally, if END log record, then clean up Xact Table
                else if (currentRecord.type == LogType.END_TRANSACTION) {
                    XactTableEntry.transaction.cleanup();
                    XactTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(XID);
                }
            }
            // Handle BEGIN_CHECKPOINT
            if (currentRecord.type == LogType.BEGIN_CHECKPOINT) {
                updateTransactionCounter.accept(currentRecord.getMaxTransactionNum().get());
            }
            /* If the log record is an end_checkpoint record:
             * - Copy all entries of checkpoint DPT (replace existing entries if any)
             * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
             *   add to transaction table if not already present.
             * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
             *   transaction table if the transaction has not finished yet, and acquire X locks. */
            else if (currentRecord.type.equals(LogType.END_CHECKPOINT)) {
                //Copy all entries of checkpoint DPT to actual DPT.
                for (Map.Entry<Long, Long> checkptDPTEntry : currentRecord.getDirtyPageTable().entrySet()) {
                    dirtyPageTable.put(checkptDPTEntry.getKey(), checkptDPTEntry.getValue());
                }
                //For each Xact in endcheckpt Xact table, Update lastLSNs in actual Xact table to max(actual LSN, checkptLSN)
                Set<Map.Entry<Long, Pair<Transaction.Status, Long>>> checkptXactTable = currentRecord.getTransactionTable().entrySet();
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> checkptXactEntry : checkptXactTable) {
                    Transaction.Status XactStatus = checkptXactEntry.getValue().getFirst();
                    long EC_lastLSN = checkptXactEntry.getValue().getSecond();//end checkpoint lastLSN for this Xact.
                    long EC_XID = checkptXactEntry.getKey();
                    //get corresponding Xact row (possibly null) in actual Xact table
                    TransactionTableEntry XactTableEntry = transactionTable.get(EC_XID);
                    if (XactStatus.equals(Status.ABORTING)) {
                        transactionTable.get(EC_XID).transaction.setStatus(Status.RECOVERY_ABORTING);
                        continue;
                    }
                    if (transactionTable.containsKey(EC_XID)) {
                        if (XactTableEntry.lastLSN < EC_lastLSN) {
                            XactTableEntry.lastLSN = EC_lastLSN;
                        }
                    }
                    //If the EC Xact table contains an Xact that doesn't exist, add it.
                    else {
                        transactionTable.put(EC_XID, new TransactionTableEntry(newTransaction.apply(EC_XID)));
                        TransactionTableEntry newRow = transactionTable.get(EC_XID);
                        newRow.lastLSN = currentRecord.getLSN();
                    }

                }
                //Add checkpoint's touchedPages to our actual Xact table's touched pages, and acquire XLOX
                Set<Map.Entry<Long, List<Long>>> checkptTouchedPagesTable = currentRecord.getTransactionTouchedPages().entrySet();
                for (Map.Entry<Long, List<Long>> touchedPagesRow : checkptTouchedPagesTable) {
                    long checkptXID = touchedPagesRow.getKey(); //XID from CHECKPOINT.
                    List<Long> checkptTouchedPages = touchedPagesRow.getValue();
                    TransactionTableEntry XactTableEntry = transactionTable.get(checkptXID);
                    if (XactTableEntry.transaction.getStatus() == Status.COMPLETE) {
                        continue;
                    }
                    //Iterate through checkpoint pages and acquire X locks on those that match.
                    for (long pageID : checkptTouchedPages) {
                        Set<Long> touchedPages = XactTableEntry.touchedPages;
                        if (!touchedPages.contains(pageID)) {
                            touchedPages.add(pageID);
                            acquireTransactionLock(XactTableEntry.transaction, getPageLockContext(pageID), LockType.X);
                        }
                    }
                }
            }
        }
        // Clean up + end COMMITTING Xacts, and update RUNNING Xacts -> RECOVERY_ABORTING.
        for (Map.Entry<Long, TransactionTableEntry> XactTableRow : transactionTable.entrySet()) {
            long XID = XactTableRow.getKey();
            TransactionTableEntry XactTableEntry = XactTableRow.getValue();
            if (XactTableEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                XactTableEntry.transaction.cleanup();
                XactTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                //Write end record to log + remove Xact from Xact table.
                logManager.appendToLog(new EndTransactionLogRecord(XID, XactTableEntry.lastLSN));
                transactionTable.remove(XID);
            }
            else if (XactTableEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                //Abort running Xacts and log abort records.
                XactTableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortLogRecord = new AbortTransactionLogRecord(XID, XactTableEntry.lastLSN);
                logManager.appendToLog(abortLogRecord);
                XactTableEntry.lastLSN = abortLogRecord.getLSN();
            }
        }

        return;
    }

    /** Return if current log record flushes changes to disk, i.e. we can remove the associated page
     *  from the DPT.**/
    private boolean flushesToDisk(LogRecord currentRecord) {
        return currentRecord.type == LogType.ALLOC_PAGE || currentRecord.type == LogType.FREE_PAGE
                || currentRecord.type == LogType.UNDO_ALLOC_PAGE || currentRecord.type == LogType.UNDO_FREE_PAGE;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(hw5): implement
        Long minRecLSN = getMinRecLSN();
        Iterator<LogRecord> iter = logManager.scanFrom(minRecLSN);
        while (iter.hasNext()) {
            LogRecord currentRecord = iter.next();
            if (currentRecord.isRedoable() && pageRelated(currentRecord)) {
                long pageID = currentRecord.getPageNum().get();
                if (dirtyPageTable.containsKey(pageID)) {
                    long recLSN = dirtyPageTable.get(pageID);
                    long LSN = currentRecord.getLSN();
                    if (LSN >= recLSN) {
                        Page diskPage = bufferManager.fetchPage(getPageLockContext(pageID).parentContext(), pageID, false);
                        if (diskPage.getPageLSN() < currentRecord.getLSN()) {
                            currentRecord.redo(diskSpaceManager, bufferManager);
                        }
                    }
                }
            }
            else if (currentRecord.isRedoable() && partitionRelated(currentRecord)) {
                currentRecord.redo(diskSpaceManager, bufferManager);
            }
        }
    }
    /** Return minimum recLSN in DPT **/
    private long getMinRecLSN(){
        long recLSN = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> DPTRow : dirtyPageTable.entrySet()) {
            if (DPTRow.getValue() < recLSN) {
                recLSN = DPTRow.getValue();
            }
        }
        return recLSN;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        //Create PQueue of Xacts which map XIDs to LASTLSNs in the log
        PriorityQueue<Pair<Long, Long>> abortingXacts = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for (Map.Entry<Long, TransactionTableEntry> XactTableRow : transactionTable.entrySet()) {
            Transaction Xact = XactTableRow.getValue().transaction;
            //ADD aborting xact row to aborting Xact PQ.
            if (Xact.getStatus() == Status.RECOVERY_ABORTING) {
                abortingXacts.add(new Pair<>(XactTableRow.getValue().lastLSN, XactTableRow.getKey()));
            }
        }

        while (abortingXacts.size() > 0) {
            Pair<Long, Long> currentLargestLSNXact = abortingXacts.poll();
            long lastLSN = currentLargestLSNXact.getFirst();
            long XID = currentLargestLSNXact.getSecond();
            LogRecord currentLogRecord = logManager.fetchLogRecord(lastLSN);
            //First, get the Xact table entry corresponding to our Log Record.
            TransactionTableEntry XactTableEntry = transactionTable.get(XID);
//            currentLogRecord.getPageNum().get(); //Page that this log rec
            if (currentLogRecord.isUndoable()) {
                LogRecord CLRRecord = currentLogRecord.undo(XactTableEntry.lastLSN).getFirst();
                boolean flush = currentLogRecord.undo(XactTableEntry.lastLSN).getSecond();
                logManager.appendToLog(CLRRecord);
                if (flush) pageFlushHook(CLRRecord.getLSN());
                XactTableEntry.lastLSN = CLRRecord.getLSN();
                CLRRecord.redo(diskSpaceManager, bufferManager);
            }
            //Update DPT accordingly.
            if (pageRelated(currentLogRecord)) {
                long pageID = currentLogRecord.getPageNum().get();
                if (dirtyPageTable.containsKey(pageID)) {
                    if (currentLogRecord.getLSN() == dirtyPageTable.get(pageID)) {
                        dirtyPageTable.remove(pageID);
                    }
                }
            }
            //Acquire newLSN (next log record to undo)
            long newLSN;
            if (currentLogRecord.getUndoNextLSN().isPresent()) {
                newLSN = currentLogRecord.getUndoNextLSN().get();
            }
            else {
                newLSN = currentLogRecord.getPrevLSN().get();
            }
            if (newLSN == 0) {
                XactTableEntry.transaction.cleanup();
                XactTableEntry.transaction.setStatus(Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(XID, XactTableEntry.lastLSN));
                //Remove from queue + Xact table
                transactionTable.remove(XID);
                abortingXacts.remove(XID);
            }
            else {
                abortingXacts.add(new Pair<>(newLSN, XID));
            }
        }
        return;
    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////
    /** Return if record is partition related.**/
    boolean partitionRelated(LogRecord record) {
        return record.getPartNum().isPresent();
    }


    /** Return if record is page related.**/
    private boolean pageRelated(LogRecord record) {
        return record.getPageNum().isPresent();
    }


    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                        lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}