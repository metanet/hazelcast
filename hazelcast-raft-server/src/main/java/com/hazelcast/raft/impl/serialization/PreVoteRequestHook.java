package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.PreVoteRequest;

import java.io.IOException;

public class PreVoteRequestHook implements SerializerHook<PreVoteRequest> {
    @Override
    public Class<PreVoteRequest> getSerializationType() {
        return PreVoteRequest.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<PreVoteRequest>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.PRE_VOTE_REQUEST;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, PreVoteRequest o) throws IOException {
                out.writeObject(o.candidate());
                out.writeInt(o.nextTerm());
                out.writeInt(o.lastLogTerm());
                out.writeLong(o.lastLogIndex());
            }

            @Override
            public PreVoteRequest read(ObjectDataInput in) throws IOException {
                return new PreVoteRequest((RaftEndpointImpl) in.readObject(), in.readInt(), in.readInt(), in.readLong());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
