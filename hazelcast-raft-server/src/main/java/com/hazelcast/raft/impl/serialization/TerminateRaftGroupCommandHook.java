package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.command.TerminateRaftGroupCommand;

public class TerminateRaftGroupCommandHook implements SerializerHook<TerminateRaftGroupCommand> {
    @Override
    public Class<TerminateRaftGroupCommand> getSerializationType() {
        return TerminateRaftGroupCommand.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<TerminateRaftGroupCommand>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.TERMINATE_RAFT_GROUP_OP;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, TerminateRaftGroupCommand o) {
            }

            @Override
            public TerminateRaftGroupCommand read(ObjectDataInput in) {
                return new TerminateRaftGroupCommand();
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
