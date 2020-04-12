package edu.berkeley.cs186.database.concurrency;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT (resource).
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        //First, acquire current transaction.
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null) return;
        //Next, acquire lock on lock context (if any). Check duplicate.
        LockType currentLT = lockContext.getExplicitLockType(transaction);
        LockType impliedLT = lockContext.getEffectiveLockType(transaction); //We'll need this in the case of calling this on child nodes.
        if (currentLT == lockType) return;
        //Now, based on the REQUIRED LockType, check CURRENT resource lock type (held by transaction).
        switch(lockType) {
            case NL: break;
            case S:
                if (isSIXSX(impliedLT)) break; //Current node has correct implicit lock
                handleS(lockContext);
                break;
            case X:
                if(impliedLT == LockType.X) break; //Current node has correct implicit lock
                handleX(lockContext);
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

    /** Applies lock LOCKTYPE to all LockContexts specified in deque RESOURCES.
    Assume RESOURCES do not have conflicting locks. **/
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
    /** Promotion + Escalation in the case of needing a shared lock on resource. **/
    private static void handleS(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        //First, we must change the parent lock -> IS such that lock can be placed on current resource.
        LockContext parentNode = lc.parentContext();
        //No parent exists for lock context.
        if (parentNode != null) {
            ArrayDeque<LockContext> noLockParents = getNoLockParents(lc);
            //Place IS locks on all parents.
            applyLocks(noLockParents, LockType.IS);
        }

        //There is also a special case: if we got an IX lock going on either in current node/children,
        // a SIX lock must be acquired on current resource,
        //AND all subtree nodes with locks != X MUST be released.
        boolean IXLockOnChild = lc.specificLockOnChild(transaction, LockType.IX);
        LockType currentLT = lc.getExplicitLockType(transaction);
        if (IXLockOnChild || currentLT == LockType.IX) {
            List<ResourceName> SISCHILDREN = lc.sisDescendants(transaction);
            SISCHILDREN.add(lc.name);
            //Acquire SIX lock on current resource and release all children with S/IS locks.
            lc.lockman.acquireAndRelease(transaction, lc.name, LockType.SIX, SISCHILDREN);
        }
        //Case 2: If current resource has no lock, then simply grant it the S lock.
        else if (currentLT == LockType.NL) {
            lc.acquire(transaction, LockType.S);
        }
        else {
            lc.escalate(transaction);
        }
        return;
    }

    /** Promotion + Escalation in the case of needing a lock on resource. **/
    private static void handleX(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parentNode = lc.parentContext();
        if (parentNode == null) return;
        boolean ISLockOnParent = lc.specificLockOnParent(transaction, LockType.IS);
        boolean SLockOnParent = lc.specificLockOnParent(transaction, LockType.S);
        if (ISLockOnParent || SLockOnParent) {
            //Promote parent nodes with IS/S locks to IX/X locks (respectively)
            promoteParents(lc);
        }
        else {
            ArrayDeque<LockContext> NoLockParents = getNoLockParents(lc);
            applyLocks(NoLockParents, LockType.IX);
        }
        LockType currentLT = lc.getEffectiveLockType(transaction);
        if (currentLT == LockType.X) return;
        BubbleUpXLock(lc);
        return;
    }

    /** Promote parents of lockcontexts' locks held by transaction: S-> X, IS->IX **/
    private static void promoteParents(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parentNode = lc.parentContext();
        ArrayDeque<LockContext> ISLockParents = new ArrayDeque<>();
        ArrayDeque<LockContext> SLockParents = new ArrayDeque<>();
        LockType parentLT;
        //Traverse up parents and add all WITH S or IS locks. Rank by parent level.
        while (parentNode != null) {
            parentLT = parentNode.getExplicitLockType(transaction);
            if (parentLT == LockType.IS) {
                ISLockParents.addFirst(parentNode);
            }
            else if (parentLT == LockType.S) {
                SLockParents.addFirst(parentNode);
            }
            parentNode = parentNode.parentContext();
        }
        //Promote all parent Locks with IS -> IX, and all parent locks with S -> X.
        promoteLocks(ISLockParents, transaction, LockType.IX);
        promoteLocks(SLockParents, transaction, LockType.X);
        return;
    }

    /** Helper method that either sets current resource lock to X or bubbles X lock up from descendants.
     * We need this because an X lock action NEEDS to have the parent node be as permissive as possible: i.e. X locked.  **/
    private static void BubbleUpXLock(LockContext lc) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockType currentLT = lc.getExplicitLockType(transaction);
        if (currentLT == LockType.NL) {
            lc.acquire(transaction, LockType.X);
        }
        else {
            lc.escalate(transaction);
            if (lc.getExplicitLockType(transaction) != LockType.X) { //Must call explicitlocktype again, since our current lock might have changed.
                lc.promote(transaction, LockType.X);
            }
        }
        return;
    }
}