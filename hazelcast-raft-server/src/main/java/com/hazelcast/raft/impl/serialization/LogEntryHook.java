package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.log.LogEntry;

import java.io.IOException;

public class LogEntryHook implements SerializerHook<LogEntry> {
    @Override
    public Class<LogEntry> getSerializationType() {
        return LogEntry.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<LogEntry>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.LOG_ENTRY;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, LogEntry o) throws IOException {
                out.writeInt(o.term());
                out.writeLong(o.index());
                out.writeObject(o.operation());
            }

            @Override
            public LogEntry read(ObjectDataInput in) throws IOException {
                return new LogEntry(in.readInt(), in.readLong(), in.readObject());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
