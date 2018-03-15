package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.VoteResponse;

import java.io.IOException;

public class VoteResponseHook implements SerializerHook<VoteResponse> {
    @Override
    public Class<VoteResponse> getSerializationType() {
        return VoteResponse.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<VoteResponse>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.VOTE_RESPONSE;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, VoteResponse o) throws IOException {
                out.writeObject(o.voter());
                out.writeInt(o.term());
                out.writeBoolean(o.granted());
            }

            @Override
            public VoteResponse read(ObjectDataInput in) throws IOException {
                return new VoteResponse((RaftEndpointImpl) in.readObject(), in.readInt(), in.readBoolean());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
