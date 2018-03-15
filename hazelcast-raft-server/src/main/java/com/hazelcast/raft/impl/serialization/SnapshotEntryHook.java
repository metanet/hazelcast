package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.SnapshotEntry;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

public class SnapshotEntryHook implements SerializerHook<SnapshotEntry> {
    @Override
    public Class<SnapshotEntry> getSerializationType() {
        return SnapshotEntry.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<SnapshotEntry>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.SNAPSHOT_ENTRY;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, SnapshotEntry o)
                    throws IOException {
                out.writeInt(o.term());
                out.writeLong(o.index());
                out.writeObject(o.operation());
                out.writeLong(o.groupMembersLogIndex());
                out.writeInt(o.groupMembers().size());
                for (RaftEndpoint endpoint : o.groupMembers()) {
                    out.writeObject(endpoint);
                }
            }

            @Override
            public SnapshotEntry read(ObjectDataInput in) throws IOException {
                int term = in.readInt();
                long index = in.readLong();
                Object op = in.readObject();
                long groupMembersLogIndex = in.readLong();
                int count = in.readInt();
                Collection<RaftEndpoint> groupMembers = new HashSet<RaftEndpoint>(count);
                for (int i = 0; i < count; i++) {
                    RaftEndpoint endpoint = in.readObject();
                    groupMembers.add(endpoint);
                }
                return new SnapshotEntry(term, index, op, groupMembersLogIndex, groupMembers);
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
