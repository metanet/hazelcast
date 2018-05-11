package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.session.RaftSessionDataSerializerHook;
import com.hazelcast.raft.service.session.RaftSessionService;

public class KeepAliveSessionOp extends AbstractSessionOp implements IdentifiedDataSerializable {

    public KeepAliveSessionOp() {
    }

    public KeepAliveSessionOp(RaftGroupId groupId, long sessionId) {
        super(groupId, sessionId);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftSessionService service = getService();
        return service.keepAlive(groupId, sessionId);
    }

    @Override
    public int getFactoryId() {
        return RaftSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionDataSerializerHook.KEEP_ALIVE_SESSION;
    }
}
