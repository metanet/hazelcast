package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.operation.RaftGroupAwareOp;
import com.hazelcast.raft.service.session.RaftSessionDataSerializerHook;
import com.hazelcast.raft.service.session.RaftSessionService;

import java.util.Map;

public class ExpireSessionsOp extends RaftGroupAwareOp implements IdentifiedDataSerializable {

    private Map<Long, Long> sessionExpirationTimes;

    public ExpireSessionsOp() {
    }

    public ExpireSessionsOp(RaftGroupId groupId, Map<Long, Long> sessionExpirationTimes) {
        super(groupId);
        this.sessionExpirationTimes = sessionExpirationTimes;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftSessionService service = getService();
        return service.expireSessions(groupId, sessionExpirationTimes);
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
        return RaftSessionDataSerializerHook.EXPIRE_SESSIONS;
    }
}
