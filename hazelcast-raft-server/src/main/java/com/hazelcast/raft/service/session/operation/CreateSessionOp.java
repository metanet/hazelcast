package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.operation.RaftGroupAwareOp;
import com.hazelcast.raft.service.session.RaftSessionDataSerializerHook;
import com.hazelcast.raft.service.session.RaftSessionService;

public class CreateSessionOp extends RaftGroupAwareOp implements IdentifiedDataSerializable {

    public CreateSessionOp() {
    }

    public CreateSessionOp(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected Object doRun(long commitIndex)
            throws Exception {
        RaftSessionService service = getService();
        return service.createNewSession(groupId);
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
        return RaftSessionDataSerializerHook.CREATE_SESSION;
    }
}
