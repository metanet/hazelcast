package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.VoteRequest;

import java.io.IOException;

public class VoteRequestHook implements SerializerHook<VoteRequest> {
    @Override
    public Class<VoteRequest> getSerializationType() {
        return VoteRequest.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<VoteRequest>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.VOTE_REQUEST;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, VoteRequest o) throws IOException {
                out.writeObject(o.candidate());
                out.writeInt(o.term());
                out.writeInt(o.lastLogTerm());
                out.writeLong(o.lastLogIndex());
            }

            @Override
            public VoteRequest read(ObjectDataInput in) throws IOException {
                return new VoteRequest((RaftEndpointImpl) in.readObject(), in.readInt(), in.readInt(), in.readLong());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
