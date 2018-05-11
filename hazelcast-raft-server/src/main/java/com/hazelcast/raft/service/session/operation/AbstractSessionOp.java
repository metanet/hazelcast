package com.hazelcast.raft.service.session.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.operation.RaftGroupAwareOp;
import com.hazelcast.raft.service.session.RaftSessionService;

import java.io.IOException;

public abstract class AbstractSessionOp extends RaftGroupAwareOp {

    long sessionId;

    public AbstractSessionOp() {
    }

    public AbstractSessionOp(RaftGroupId groupId, long sessionId) {
        super(groupId);
        this.sessionId = sessionId;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeLong(sessionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        sessionId = in.readLong();
    }
}
