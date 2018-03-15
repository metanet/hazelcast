package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;

import java.io.IOException;

public class AppendFailureResponseHook implements SerializerHook<AppendFailureResponse> {
    @Override
    public Class<AppendFailureResponse> getSerializationType() {
        return AppendFailureResponse.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<AppendFailureResponse>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.APPEND_FAILURE_RESPONSE;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, AppendFailureResponse o) throws IOException {
                out.writeObject(o.follower());
                out.writeInt(o.term());
                out.writeLong(o.expectedNextIndex());
            }

            @Override
            public AppendFailureResponse read(ObjectDataInput in) throws IOException {
                return new AppendFailureResponse((RaftEndpointImpl) in.readObject(), in.readInt(), in.readLong());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
