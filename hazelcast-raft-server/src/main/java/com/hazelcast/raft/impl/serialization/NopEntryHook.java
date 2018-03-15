package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.log.NopEntry;

public class NopEntryHook implements SerializerHook<NopEntry> {
    @Override
    public Class<NopEntry> getSerializationType() {
        return NopEntry.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<NopEntry>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.NOP_ENTRY_OP;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, NopEntry o) {
            }

            @Override
            public NopEntry read(ObjectDataInput in) {
                return new NopEntry();
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
