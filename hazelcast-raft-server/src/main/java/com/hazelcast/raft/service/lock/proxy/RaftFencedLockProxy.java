package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.operation.GetLockCountOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.session.SessionManagerService;

import java.util.UUID;
import java.util.concurrent.Future;

public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    private final RaftInvocationManager invocationManager;

    public RaftFencedLockProxy(String name, RaftGroupId groupId, SessionManagerService sessionManager,
                               RaftInvocationManager invocationManager) {
        super(name, groupId, sessionManager);
        this.invocationManager = invocationManager;
    }

    @Override
    protected Future<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        RaftOp op = new LockOp(name, sessionId, threadId, invocationUid);
        return invocationManager.invoke(groupId, op);
    }

    @Override
    protected Future<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid,
                                     long timeoutMillis) {
        RaftOp op = new TryLockOp(name, sessionId, threadId, invocationUid, timeoutMillis);
        return invocationManager.invoke(groupId, op);
    }

    @Override
    protected Future<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        return invocationManager.invoke(groupId, new UnlockOp(name, sessionId, threadId, invocationUid));
    }

    @Override
    protected Future<Object> doForceUnlock(RaftGroupId groupId, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Future<Integer> doGetLockCount(RaftGroupId groupId, String name) {
        return invocationManager.invoke(groupId, new GetLockCountOp(name));
    }
}
