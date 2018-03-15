package com.hazelcast.raft.impl.log;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;

/**
 * Represents a snapshot in the {@link RaftLog}.
 * <p>
 * Snapshot entry is sent to followers via {@link com.hazelcast.raft.impl.dto.InstallSnapshot} RPC.
 */
public class SnapshotEntry extends LogEntry {
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, long index, Object operation,
            long groupMembersLogIndex, Collection<RaftEndpoint> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    @Override
    public String toString() {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index() + ", operation=" + operation()
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }
}
