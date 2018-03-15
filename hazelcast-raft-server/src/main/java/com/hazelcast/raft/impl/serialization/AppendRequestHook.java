package com.hazelcast.raft.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.log.LogEntry;

import java.io.IOException;

public class AppendRequestHook implements SerializerHook<AppendRequest> {
    @Override
    public Class<AppendRequest> getSerializationType() {
        return AppendRequest.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<AppendRequest>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.APPEND_REQUEST;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, AppendRequest o) throws IOException {
                out.writeObject(o.leader());
                out.writeInt(o.term());
                out.writeInt(o.prevLogTerm());
                out.writeLong(o.prevLogIndex());
                out.writeLong(o.leaderCommitIndex());

                LogEntry[] entries = o.entries();
                out.writeInt(entries.length);
                for (LogEntry entry : entries) {
                    out.writeObject(entry);
                }
            }

            @Override
            public AppendRequest read(ObjectDataInput in) throws IOException {
                RaftEndpointImpl leader = in.readObject();
                int term = in.readInt();
                int prevLogTerm = in.readInt();
                long prevLogIndex = in.readLong();
                long leaderCommitIndex = in.readLong();
                int count = in.readInt();
                LogEntry[] entries = new LogEntry[count];
                for (int i = 0; i < count; i++) {
                    entries[i] = in.readObject();
                }
                return new AppendRequest(leader, term, prevLogTerm, prevLogIndex, leaderCommitIndex, entries);
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
