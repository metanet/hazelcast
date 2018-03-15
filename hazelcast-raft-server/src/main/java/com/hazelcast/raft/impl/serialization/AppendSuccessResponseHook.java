package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;

import java.io.IOException;

public class AppendSuccessResponseHook implements SerializerHook<AppendSuccessResponse> {
    @Override
    public Class<AppendSuccessResponse> getSerializationType() {
        return AppendSuccessResponse.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<AppendSuccessResponse>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.APPEND_SUCCESS_RESPONSE;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, AppendSuccessResponse o) throws IOException {
                out.writeObject(o.follower());
                out.writeInt(o.term());
                out.writeLong(o.lastLogIndex());
            }

            @Override
            public AppendSuccessResponse read(ObjectDataInput in) throws IOException {
                return new AppendSuccessResponse((RaftEndpointImpl) in.readObject(), in.readInt(), in.readLong());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
