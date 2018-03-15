package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.log.SnapshotEntry;

import java.io.IOException;

public class InstallSnapshotHook implements SerializerHook<InstallSnapshot> {
    @Override
    public Class<InstallSnapshot> getSerializationType() {
        return InstallSnapshot.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<InstallSnapshot>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.INSTALL_SNAPSHOT;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, InstallSnapshot o) throws IOException {
                out.writeObject(o.leader());
                out.writeInt(o.term());
                out.writeObject(o.snapshot());
            }

            @Override
            public InstallSnapshot read(ObjectDataInput in) throws IOException {
                return new InstallSnapshot((RaftEndpointImpl) in.readObject(), in.readInt(), (SnapshotEntry) in.readObject());
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
