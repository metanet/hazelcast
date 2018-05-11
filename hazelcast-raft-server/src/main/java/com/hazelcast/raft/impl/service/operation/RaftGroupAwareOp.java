package com.hazelcast.raft.impl.service.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;

import java.io.IOException;

public abstract class RaftGroupAwareOp
        extends RaftOp {

    protected RaftGroupId groupId;

    public RaftGroupAwareOp() {
    }

    public RaftGroupAwareOp(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
    }

}
