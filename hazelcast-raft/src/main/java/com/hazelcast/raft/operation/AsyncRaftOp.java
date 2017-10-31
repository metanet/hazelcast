package com.hazelcast.raft.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class AsyncRaftOp extends Operation {

    String name;
//    int term;


    public AsyncRaftOp() {
    }

    public AsyncRaftOp(String name) {
        this.name = name;
    }

    RaftResponseHandler newResponseHandler() {
        return new RaftResponseHandler(this);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
//        out.writeInt(term);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
//        term = in.readInt();
    }
}
