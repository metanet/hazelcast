package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.PreVoteResponse;

import java.io.IOException;

public class PreVoteResponseHook implements SerializerHook<PreVoteResponse> {
    @Override
    public Class<PreVoteResponse> getSerializationType() {
        return PreVoteResponse.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<PreVoteResponse>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.PRE_VOTE_RESPONSE;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, PreVoteResponse o) throws IOException {
                out.writeObject(o.voter());
                out.writeInt(o.term());
                out.writeBoolean(o.granted());
            }

            @Override
            public PreVoteResponse read(ObjectDataInput in) throws IOException {
                return new PreVoteResponse((RaftEndpointImpl) in.readObject(), in.readInt(), in.readBoolean());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
