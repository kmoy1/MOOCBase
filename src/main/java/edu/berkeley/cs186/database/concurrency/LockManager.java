package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        /**
         * Get lock type of active lock.
         * @return LockType
         */
        public LockType activeLockType() {
            if (locks.isEmpty()) {
                return LockType.NL;
            }
            Lock current = locks.get(0);
            return current.lockType;
        }
        /**
         * Check for ability of a transaction to add a new lock on this resource.
         * This does the standard compatibility check with all locks on the resource.
         */
        public boolean canAddLock(TransactionContext t, LockType lockType) {
            for (Lock lock: locks) {
                //If even one lock in resource's active locks is incompatible with locktype, can't add lock.
                if (lock.transactionNum != t.getTransNum() && !LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Check for ability of a transaction to promote resource lock. NO PROMOTIONS TO SIX (acquireAndRelease only).
         * When promoting to SIX, we must release redundant S/IS locks on children.
         * @return true if our transaction can PROMOTE the current lock.
         */
        public boolean promotable(TransactionContext t, LockType newLockType) {
            if (locks.size() != 1) return false;
            Lock current = locks.get(0);
            return canAddLock(t, newLockType) && t.getTransNum() == current.transactionNum;
        }

        /**
         * return true if our current lock is compatible with LOCKTYPE, i.e.
         * if a LOCKTYPE lock can be shared with current locktype.
         * There also must be no other transactions in queue for this resource for compatibility.
         */
        public boolean compatible(LockType lockType) {
            return LockType.compatible(activeLockType(), lockType) &&
                    waitingQueue.isEmpty();
        }

        /**
         * Promote active lock to transaction t's newLockType.
         */
        public void promoteLock(TransactionContext t, LockType newLockType) {
            assert promotable(t, newLockType);
            Lock current = locks.get(0);
            current.lockType = newLockType;
        }
        /**Helper function to convert list of locks to list of associated resource names. **/
        public List<ResourceName> locksToNames(List<Lock> locks) {
            List<ResourceName> names = new ArrayList<>();
            for (Lock l: locks) {
                ResourceName name = l.name;//Locked resource for queued lock
                names.add(name);
            }
            return names;
        }
        /** For Transaction T's locks, release every lock
         * specified by a lock request's releasedLocks. **/
        public void releaseAllLocks(LockRequest lrq, TransactionContext t) {
            for (Lock releasedLock: lrq.releasedLocks) {
                ResourceName locked = releasedLock.name;//Locked resource for queued lock
                release(t, locked);
            }
        }
        /**
         * Remove LOCK from active locks on resource, and replace appropriately from lock request queue.
         * @param lock lock to boot.
         * @throws NoLockHeldException
         */
        public void removeLock(Lock lock) throws NoLockHeldException {
            if (!locks.remove(lock)) {
                throw new NoLockHeldException("Can't Remove No Lock.");
            }
            //We will now iterate through all locks in waitingQueue until a suitable replacement is found.
            //When it is, we shall make it active and unblock the corresponding transaction.
            while (!waitingQueue.isEmpty()) {
                LockRequest lrq = waitingQueue.getFirst();
                TransactionContext waitingTransaction = lrq.transaction;
                if (!canAddLock(waitingTransaction, lrq.lock.lockType)) {
                    break; //Immediately break if lock doesn't match with transaction.
                }
                long tNum = waitingTransaction.getTransNum();
                // Release everything in RELEASEDLOCKS for this LockReq
                releaseAllLocks(lrq, waitingTransaction);
                //Remove active lock from the transaction's locks, if it exists.
                // If this ends up emptying the transaction's locks, we can release
                // that transaction entirely.
                if (promotable(lrq.transaction, lrq.lock.lockType)) {
                    Lock current = locks.get(0);
                    List<Lock> tLox = transactionLockMasterList(tNum);
                    tLox.remove(current);
                    if (tLox.isEmpty()) {
                        transactionLocks.remove(tNum);
                    }
                    locks.clear();
                }
                //Finally, activate lock and unblock transaction, and REMOVE lock request from queue
                tAddLock(tNum, lrq.lock);
                locks.add(lrq.lock);
                waitingTransaction.unblock();
                waitingQueue.remove();
            }
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Get resource entry from name.
     * If it doesn't exist, create a mapping -> new ResourceEntry object and return it.
     * @param name resource name.
     * @return resource entry.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        if (resourceEntries.containsKey(name)) {
            return resourceEntries.get(name);
        }
        resourceEntries.put(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Return (a pointer to) the official list of locks for a transaction.
     * Again, if the transaction does not exist, map anew.
     * @param transactionNum transaction number
     * @return lock list
     */
    private List<Lock> transactionLockMasterList(long transactionNum) {
        if (transactionLocks.containsKey(transactionNum)) {
            return transactionLocks.get(transactionNum);
        }
        transactionLocks.put(transactionNum, new ArrayList<>());
        return transactionLocks.get(transactionNum);
    }


    /**
     * Adds a lock to a transaction's master list.
     * @param transactionNum transaction number
     * @param lock lock to add
     */
    private void tAddLock(long transactionNum, Lock lock) {
        transactionLockMasterList(transactionNum).add(lock);
    }

    /**
     * Promote a specific LOCK to LOCKTYPE for this transaction
     * (update master list in transactionLocks)
     * @param transactionNum transaction number
     * @param lock lock to remove
     */
    private void tPromoteLock(long transactionNum, Lock lock, LockType newLockType) {
        List<Lock> masterList = transactionLockMasterList(transactionNum);
        Lock l = masterList.get(masterList.indexOf(lock));
        l.lockType = newLockType;
    }

    /**
     * Remove's a lock from a transaction's master list.
     * If the transaction holds no more locks on resources, remove entirely.
     * @param transactionNum transaction number
     * @param lock lock to remove
     */
    private void tRemoveLock(long transactionNum, Lock lock) {
        List<Lock> masterList = transactionLockMasterList(transactionNum);
        masterList.remove(lock);
        if (masterList.isEmpty()) {
            transactionLocks.remove(transactionNum);
        }
    }

    /**
     * Return a list of a transaction's actual locks to be released
     * that correspond to the resources specified in (release locks).
     */
    private List<Lock> actualReleaseLocks(TransactionContext transaction,
                                          List<ResourceName> rl) {
        List<Lock> lox = new ArrayList<>();
        long tNum;
        for (ResourceName name: rl) {
            tNum = transaction.getTransNum();
            LockType lt = getLockType(transaction, name);
            Lock lock = new Lock(name, lt, tNum);
            lox.add(lock);
        }
        return lox;
    }

    /** Release all of a transaction's locks it holds on resources specified by RESOURCES **/
    private void removeLocksOnResources(TransactionContext transaction,
                                        List<ResourceName> resources) {
        for (ResourceName name: resources) {
            release(transaction, name);
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        boolean compatible = false; //boolean indicator on whether transaction lock is compatible with current lock(s),
                                    // determines whether transaction blocked or not.
        synchronized (this) {
            LockType lt = getLockType(transaction, name);
            //CASE 1: We try to A+R a lock on a resource that isn't meant to be released.
            if (lt != LockType.NL && !releaseLocks.contains(name)) {
                throw new DuplicateLockRequestException("Transaction already holds non-released lock on resource");
            }
            //Check for any no-locks on resources that we need to release locks from.
            for (ResourceName rname: releaseLocks) {
                if (getLockType(transaction, rname) == LockType.NL) {
                    throw new NoLockHeldException("No Transaction Lock Exists on a resource in releaselocks.");
                }
            }
            //Create lock to add.
            ResourceEntry resource = getResourceEntry(name);
            long tNum = transaction.getTransNum(); //transaction number
            Lock lockAR = new Lock(name, lockType, tNum);
            //Check lock compatibility with all other transaction locks on resource.
            //Lock compatible: Activate requested lock and release all releaselocks.
            if (resource.canAddLock(transaction, lockType)) {
                compatible = true;
                removeLocksOnResources(transaction, releaseLocks);
                resource.locks.add(lockAR);
                tAddLock(tNum, lockAR);
            }
            else { //Lock NOT compatible: Make a Lock Request and add to front of waiting queue.
                List<Lock> sameReleaseLocks = actualReleaseLocks(transaction, releaseLocks);
                LockRequest lrq = new LockRequest(transaction, lockAR, sameReleaseLocks);
                resource.waitingQueue.addFirst(lrq);
            }
        }
        if (!compatible) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**f
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean compatible = false;
        LockType lt = getLockType(transaction, name);
        synchronized (this) {
            if (lt != LockType.NL) {
                throw new DuplicateLockRequestException("Lock Already Held By Transaction on Resource");
            }
            long tNum = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            Lock lockToAcquire = new Lock(name, lockType, tNum);
            if (resource.compatible(lockType)) {
                compatible = true;
                resource.locks.add(lockToAcquire);
                tAddLock(tNum, lockToAcquire);
            }
            else {
                LockRequest lrq = new LockRequest(transaction, lockToAcquire);
                resource.waitingQueue.addLast(lrq);
            }
        }
        if (!compatible){
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        LockType lt = getLockType(transaction, name);
        synchronized (this) {
            if (lt == LockType.NL) {
                throw new NoLockHeldException("Can't Release No Lock.");
            }
            long tNum = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            Lock lockToRelease = new Lock(name, lt, tNum);
            //Remove locks from transaction and resource entry.
            tRemoveLock(tNum, lockToRelease);
            resource.removeLock(lockToRelease);
        }
        transaction.unblock();
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        LockType lt = getLockType(transaction, name);
        boolean compatible = false;
        synchronized (this) {
            if (lt == newLockType) {
                throw new DuplicateLockRequestException("Lock Already Held On Resource By transaction.");
            }
            else if (lt == LockType.NL) {
                throw new NoLockHeldException("Can't Promote No Lock.");
            }
            //If promotion (substitution) is conceptually invalid.
            else if (!LockType.substitutable(newLockType, lt)) {
                throw new InvalidLockException("Promotion Invalid.");
            }
            else {
                long tNum = transaction.getTransNum();
                ResourceEntry resource = getResourceEntry(name);
                //Check compatibility (promotability) of new lock.
                //COMPATIBLE. Promote resource as well as transaction locks.
                if (resource.promotable(transaction, newLockType)) {
                    compatible = true;
                    Lock promotedLock = new Lock(name, lt, tNum);
                    tPromoteLock(tNum, promotedLock, newLockType);
                    resource.promoteLock(transaction, newLockType);
                }
                //NOT compatible/promotable. Place request at front of queue.
                else {
                    Lock L = new Lock(name, newLockType, tNum);
                    LockRequest lrq = new LockRequest(transaction, L);
                    resource.waitingQueue.addFirst(lrq);
                }
            }
        }
        if (!compatible) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        long tNum = transaction.getTransNum();
        //Search through the transaction's locks until we find one with resource name.
        for (Lock lock : transactionLockMasterList(tNum)) {
            if (lock.name.equals(name))
                return lock.lockType;
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
