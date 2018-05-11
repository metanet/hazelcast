package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.operation.RaftGroupAwareOp;
import com.hazelcast.raft.service.session.RaftSessionDataSerializerHook;
import com.hazelcast.raft.service.session.RaftSessionService;

// committed after a raft node becomes the leader
public class KeepAliveAllSessionsOp extends RaftGroupAwareOp implements IdentifiedDataSerializable {

    public KeepAliveAllSessionsOp() {
    }

    public KeepAliveAllSessionsOp(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftSessionService service = getService();
        service.keepAliveAll(groupId);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionDataSerializerHook.KEEP_ALIVE_ALL_SESSIONS;
    }

}
