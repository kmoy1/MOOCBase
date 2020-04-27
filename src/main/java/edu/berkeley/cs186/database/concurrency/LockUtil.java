package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) return;
        LockContext parent = lockContext.parentContext();
        LockType currentLT = lockContext.getExplicitLockType(transaction);
        LockType impliedLT = lockContext.getEffectiveLockType(transaction); //Parent Lock Context
        if (currentLT == lockType) return;
        //If parent-most node of lock context HAS NO lock (NOT NL LockType),
        // then we are free to place whatever lock necessary on current LC.
        if (parent == null) {
            if (currentLT == LockType.NL) lockContext.acquire(transaction, lockType);
            else{
                updateCurrentLock(transaction, lockContext, lockType);
            }
        }
        switch(lockType){
            case NL: break;
            case S:
                if (isSIXSX(impliedLT)) break; //Current node has correct implicit lock
                TopDownParentLockUpdate(transaction, lockContext, lockType);
                break;
            case X:
                if(impliedLT == LockType.X) break; //Current node has correct implicit lock
                TopDownParentLockUpdate(transaction, lockContext, lockType);
                break;
            default:
                throw new UnsupportedOperationException();//Should never reach here.
        }
        return;
    }

    /** Helper indicator if lock is one of the 3 key (redundancy) types. **/
    public static boolean isSIXSX(LockType lt) {
        return lt == LockType.SIX || lt == LockType.S || lt == LockType.X;
    }

    /**ONLY call this when operating on page-level resources that a transaction holds a lock on.
     * Perform auto escalation when two conditions are met:
     The transaction holds at least 20% of the table's pages (saturation > 20%)
     The table has at least 10 pages (to avoid locking the entire table unnecessarily when the table is very small).
     **/
    public static void autoEscalate(TransactionContext transaction, LockContext lc) {
        if (lc.isTable() && lc.saturation(transaction) >= 0.2
                && lc.capacity() >= 10) {
            lc.escalate(transaction);
        }
    }
    /**Helper method that update's a resource's (lc) lock directly,
     * depending on circumstances with the current lock it has. **/
    private static void updateCurrentLock(TransactionContext transaction, LockContext lc, LockType lockType) {
//        if (lc.readonly) return; //Seems fishy; may need to remove. Update: PASS TESTS REMOVING! :)
//        autoEscalate(transaction,lc);
        LockType currentLT = lc.getExplicitLockType(transaction);
        //CASE 1: If current resource has no lock, then simply acquire it.
        if (currentLT == LockType.NL) {
            lc.acquire(transaction, lockType);
        }
        //CASE 2: We currently hold an S lock and we want an X lock. Need to PROMOTE it.
        else if (currentLT == LockType.S && lockType == LockType.X) {
            lc.promote(transaction, lockType);
        }
        //CASE 3: We hold an IX and want an S. To handle subtree X locks, we must promote to SIX.
        // Necessary child lock releases are handled in promote.
        else if (lockType == LockType.S && currentLT == LockType.IX ) {
            lc.promote(transaction, LockType.SIX);
        }
        //CASE 4: We hold an IS/IX lock and want an S/X lock. We need to ESCALATE.
        else if ((currentLT == LockType.IX && lockType == LockType.X)||
                (currentLT == LockType.IS && lockType == LockType.S)) {
            lc.escalate(transaction);
        }
        else return; //Should never reach here.
    }

    /** Helper function that returns root of resource hierarchy.**/
    private static LockContext root(LockContext lc) {
        LockContext temp = lc;
        // traverse to top of hierarchy
        while (temp.parentContext() != null) {
            temp = temp.parentContext();
        }
        return temp;
    }

    /** Helper function that returns a list of parent resources from given lock context**/
    private static List<LockContext> parentResources(LockContext lc) {
        List<LockContext> parents = new ArrayList<>();
        LockContext resource = lc;
        // traverse to top of hierarchy
        while (resource.parentContext() != null) {
            resource = resource.parentContext();
            parents.add(resource);
        }
        parents.add(resource);
        return parents;
    }
    /** Helper method for updating PARENT locks as necessary, FROM ROOT TO CLOSEST PARENT (TOP DOWN)s.
     * In lieu of part 3,
     * I decided that using a Deque was overly cute and simply reversed the list of parents.**/
    private static void TopDownParentLockUpdate(TransactionContext transaction, LockContext lc, LockType lockType) {
        List<LockContext> parentResources = parentResources(lc);
        // Pointer to parent.
        LockContext parent;

        for (int i = parentResources.size()-1; i >= 0; i--) {
            parent = parentResources.get(i);
//            autoEscalate(transaction,lc);
            LockType parentLT = parent.getExplicitLockType(transaction);
            switch (lockType) {
                case X:
                    switch (parentLT) {
                        case X: return;
                        //Parents of X locks can only be IX/SIX/X
                        case NL: parent.acquire(transaction, LockType.IX); break;
                        case S: parent.promote(transaction, LockType.SIX); break;
                        case IS: parent.promote(transaction, LockType.IX); break;
                    }
                    break;
                case S:
                    //Parents of S locks may ONLY be S/IS.
                    if (parentLT == LockType.S) return;
                    //Same as applyLocks(noLockParents, LockType.IS)
                    if (parentLT == LockType.NL) parent.acquire(transaction, LockType.IS);
                    break;
            }
        }
        updateCurrentLock(transaction, lc, lockType);
        return;
    }

    public static void applyLocks(ArrayDeque<LockContext> resources, LockType locktype) {
        TransactionContext transaction = TransactionContext.getTransaction();
        for (LockContext resource : resources) {
            resource.acquire(transaction, locktype);
        }
    }

    /** Promotes all locks to LOCKTYPE in all LockContexts specified in deque RESOURCES.
     Assume RESOURCES do not have conflicting locks. **/
    public static void promoteLocks(ArrayDeque<LockContext> resources,
                                    TransactionContext transaction, LockType locktype) {
        for (LockContext resource : resources) {
            resource.promote(transaction, locktype);
        }
    }

    private static ArrayDeque<LockContext> getNoLockParents(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parentNode = lc.parentContext();
        ArrayDeque<LockContext> noLockParents = new ArrayDeque<>();
        LockType parentLT;
        //Traverse up parents and add all without locks. Rank by parent level.
        while (parentNode != null) {
            parentLT = parentNode.getExplicitLockType(transaction);;
            if (parentLT == LockType.NL) noLockParents.addFirst(parentNode);
            parentNode = parentNode.parentContext();
        }
        return noLockParents;
    }
    /**Solely used by lockIndexMeta: **/
    public static void lockDBAndIndices(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (lc.readonly) return;
        lockParents(LockType.IX, lc.parentContext());
    }
    /**Solely used by lockTableMeta**/
    public static void lockDBAndTables(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (lc.readonly) return; //Fishy, might wanna remove this later.
        lockParents(LockType.IX, lc.parentContext());
    }
    /**Lock parents (index/table and database) TOP-DOWN. Assume LC is not null initially.**/
    private static void lockParents(LockType lockType, LockContext lc) {
        if (lc == null) return;
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parent = lc.parentContext();
        LockType currentLT = lc.getExplicitLockType(transaction);
        if (lockType == LockType.IX || lockType == LockType.SIX) {
            if (parent != null) lockParents(LockType.IX, parent);
            //Reached the root.
            //Parent doesnt have a lock, make it IX.
            if (currentLT == LockType.NL) {
                lc.acquire(transaction, LockType.IX);
            }
            //Parent has S lock -> SIX lock (all IX locks acquired before are released)
            else if (currentLT == LockType.S) {
                lc.promote(transaction, LockType.SIX);
            }
            //Otherwise, promote to IX
            else {
                boolean SorISorNL = LockType.substitutable(currentLT, LockType.IX);
                if (!SorISorNL) { //S or IS or NL
                    lc.promote(transaction, LockType.IX);
                }
            }
        }
    }
}