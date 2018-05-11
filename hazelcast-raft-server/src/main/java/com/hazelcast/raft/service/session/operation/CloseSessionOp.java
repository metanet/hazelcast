package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.session.RaftSessionDataSerializerHook;
import com.hazelcast.raft.service.session.RaftSessionService;

public class CloseSessionOp extends AbstractSessionOp implements IdentifiedDataSerializable {

    public CloseSessionOp() {
    }

    public CloseSessionOp(RaftGroupId groupId, long sessionId) {
        super(groupId, sessionId);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftSessionService service = getService();
        service.closeSession(groupId, sessionId);
        return null;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionDataSerializerHook.CLOSE_SESSION;
    }

}
