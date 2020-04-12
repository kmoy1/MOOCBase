package edu.berkeley.cs186.database.concurrency;
<<<<<<< HEAD

import edu.berkeley.cs186.database.Transaction;
=======
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext.childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
<<<<<<< HEAD
            throws InvalidLockException, DuplicateLockRequestException {
        // Check if parent node lock is valid first.
        boolean hasParentNode = parentContext() != null;
        LockContext parentNode = parentContext();
        if (hasParentNode) {
            LockType parentLock = parentNode.getExplicitLockType(transaction); //get actual lock on parent node.
            boolean difParentLock = LockType.parentLock(lockType) != parentLock;
            boolean compatible = LockType.substitutable(parentLock, LockType.parentLock(lockType));
            boolean redundantSIX = (lockType == LockType.S || lockType == LockType.IS) && hasSIXAncestor(transaction);
            if (difParentLock && !compatible || redundantSIX) {
                throw new InvalidLockException("Invalid Lock");
            }
        }
        //Check if current lock is ok.
        if (getExplicitLockType(transaction) == lockType) throw new DuplicateLockRequestException("DupeLock");
        //Check readonly (no locks allowed)
        if (readonly) throw new UnsupportedOperationException();
        // Use Lock Manager's acquire.
        lockman.acquire(transaction, name, lockType);
        // Update parent meta
        if (hasParentNode) updateNewLocks(transaction);
    }
    /** Helper function that updates transaction's number of locks on PARENT context meta.
     *  Update numChildLocks on parent.
      */
     public void updateNewLocks(TransactionContext transaction) {
         long tNum = transaction.getTransNum();
         LockContext parentNode = parentContext();
         if (parentNode.numChildLocks.containsKey(tNum)) {
            int newNumLocks = parentNode.numChildLocks.get(tNum) + 1;
            parentNode.numChildLocks.put(tNum, newNumLocks);
        }
        //Establish transaction if new
        else {
            parentNode.numChildLocks.put(tNum, 1);
        }
=======
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        return;
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
<<<<<<< HEAD
            throws NoLockHeldException, InvalidLockException {
        LockType currentLT = getExplicitLockType(transaction);
        if (readonly) throw new UnsupportedOperationException();
        if (currentLT == LockType.NL) throw new NoLockHeldException("No Lock Held");
        long tNum = transaction.getTransNum();
        if (numChildLocks.containsKey(tNum)) {
            if (numChildLocks.get(tNum) > 0) {
                throw new InvalidLockException("Invalid Lock");
            }
        }
        //Call Lock Manager Release.
        lockman.release(transaction, name);
        //Update parent meta (MINUS 1 this time bc release) [Update numChildLocks on parent]
        boolean hasParentNode = parentContext() != null;
        LockContext parentNode = parentContext();
        if (hasParentNode) {
            int newNumLocks = parentNode.numChildLocks.get(tNum) - 1;
            parentNode.numChildLocks.put(tNum, newNumLocks);
        }
=======
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
        return;
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
<<<<<<< HEAD
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) throw new UnsupportedOperationException();
        LockType currentLT = getExplicitLockType(transaction);
        if (currentLT == LockType.NL) throw new NoLockHeldException("No Lock Held");
        if (currentLT == newLockType) throw new DuplicateLockRequestException("DupeLock");
        if (!LockType.substitutable(newLockType, currentLT)) throw new InvalidLockException("Invalid LReq");
        boolean hasParent = parentContext() != null;
        LockContext parentNode = parentContext();
        LockType correctParent = LockType.parentLock(newLockType);
        if (hasParent) {
            LockType parentLT = parentNode.getExplicitLockType(transaction); //Actual parent lock.
            if (correctParent != parentLT && !LockType.substitutable(parentLT, correctParent)) {
                throw new InvalidLockException("Invalid LReq");
            }
        }
        //Release all S/IS/IX locks simultaneously on self + descendants
        if (newLockType == LockType.SIX) {
            List<ResourceName> toBeReleased = new ArrayList<>();
            List<Lock> tLocks = lockman.getLocks(transaction);
            toBeReleased.add(name);
            for (Lock lock : tLocks) {
                ResourceName lockedResource = lock.name;
                boolean validReleaseLock = (lock.lockType == LockType.S ||
                        lock.lockType == LockType.IS || lock.lockType == LockType.IX);
                if (lockedResource.isDescendantOf(name) && validReleaseLock) {
                    toBeReleased.add(lockedResource);
                }
            }
            lockman.acquireAndRelease(transaction, name, newLockType, toBeReleased);
        }
        else {
            lockman.promote(transaction, name, newLockType);
        }
=======
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
        return;
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
<<<<<<< HEAD
        if (readonly) throw new UnsupportedOperationException();
        LockType currentLT = getExplicitLockType(transaction);
        if (currentLT == LockType.NL) throw new NoLockHeldException("No Lock Held");
        if (currentLT == LockType.S || currentLT == LockType.X) return;
        LockType escalateLockType = LockType.NL;
        List<ResourceName> releaseLocks = new ArrayList<>(); //LIst of resources to be freed of locks.
        List<Lock> tLocks = lockman.getLocks(transaction);
        long tNum = transaction.getTransNum();
        switch(currentLT) {
            case IS:
                escalateLockType = LockType.S;
                break;
            case IX:
            case SIX:
                escalateLockType = LockType.X;
                break;
            case S:
            case X:
                return;
        }
        //Add current resource to list of resources to be lock-freed.
        releaseLocks.add(name);
=======
        // TODO(proj4_part2): implement
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027

        for (Lock lock : tLocks) {
            ResourceName resource = lock.name;
            //Check if resource is a child node and has a lock. If so, add resource to list of resources to free
            if (resource.isDescendantOf(name) && lock.lockType != LockType.NL) {
                releaseLocks.add(resource);
            }
        }
        //Escalation does not bubble up anything. Release ALL locks.
        if (escalateLockType != LockType.NL) {
            lockman.acquireAndRelease(transaction, name, escalateLockType, releaseLocks);
        }
        if (numChildLocks.containsKey(tNum)) {
            updateNewLocksE(transaction, releaseLocks.size());
        }
        return;
    }

    /** Helper method to update child locks on parent. This is FOR LOCK ESCALATION,
     * which means the number of child nodes must be taken into account. **/
    private void updateNewLocksE(TransactionContext transaction, int releaseSize) {
        long tNum = transaction.getTransNum();
        int numNewLocks = numChildLocks.get(tNum) - releaseSize + 1;
        numChildLocks.put(tNum, numNewLocks);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType currentLT = getExplicitLockType(transaction);
        if (currentLT == LockType.S || currentLT == LockType.SIX) {
            return LockType.S;
        }
        else if (currentLT == LockType.X) {
            return LockType.X;
        }
        LockContext parentNode = parentContext();
        //Traverse up to parent until we find S/X/SIX lock.
        // Too lazy to do this function recursively.
        while (parentNode != null) {
            LockType nodeLT = parentNode.getExplicitLockType(transaction);
            if (nodeLT == LockType.S || nodeLT == LockType.SIX) {
                return LockType.S;
            }
            else if (nodeLT == LockType.X) {
                return LockType.X;
            }
            parentNode = parentNode.parentContext();
        }
<<<<<<< HEAD
=======
        // TODO(proj4_part2): implement
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
<<<<<<< HEAD
        for (Lock lock : lockman.getLocks(transaction)) {
            ResourceName lockedResource = lock.name;
            if (name.isDescendantOf(lockedResource) && !name.equals(lockedResource) && lock.lockType == LockType.SIX) {
                return true;
            }
        }
=======
        // TODO(proj4_part2): implement
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
<<<<<<< HEAD
    public List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> desc = new ArrayList<>();
        for (Lock lock : lockman.getLocks(transaction)) {
            ResourceName lockedResource = lock.name;
            //All locks that are S or IS AND descendants.
            if ((lock.lockType == LockType.S || lock.lockType == LockType.IS) &&
                    lockedResource.isDescendantOf(name) && lockedResource != name) {
                desc.add(lockedResource);
            }
        }
        return desc;
=======
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return new ArrayList<>();
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
<<<<<<< HEAD
        if (transaction == null) return LockType.NL;
        return lockman.getLockType(transaction, name);
=======
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        return LockType.NL;
>>>>>>> d3f1c58acb536e37b4814137e297ed49de67e027
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }


    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    public synchronized boolean isTable() {
        return this.capacity > 0;
    }

    /** Check if a transaction holds a LOCKTYPE lock on a CHILD of this lock context**/
    public boolean specificLockOnChild(TransactionContext transaction, LockType lockType) {
        for (Lock lock : lockman.getLocks(transaction)) {
            ResourceName lockedResource = lock.name;
            if (lockedResource.isDescendantOf(name) && lock.lockType == lockType) {
                return true;
            }
        }
        return false;
    }

    /** Check if a transaction holds a LOCKTYPE lock on a PARENT of this lock context**/
    public boolean specificLockOnParent(TransactionContext transaction, LockType lockType) {
        for (Lock lock : lockman.getLocks(transaction)) {
            ResourceName lockedResource = lock.name;
            if (name.isDescendantOf(lockedResource) && lock.lockType == lockType) {
                return true;
            }
        }
        return false;
    }
}
