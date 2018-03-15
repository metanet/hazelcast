package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.SnapshotEntry;

/**
 * Struct for InstallSnapshot RPC.
 * <p>
 * See <i>7 Log compaction</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to send chunks of a snapshot to a follower. Leaders always send chunks in order.
 */
public class InstallSnapshot {

    private RaftEndpoint leader;

    private int term;

    private SnapshotEntry snapshot;

    public InstallSnapshot() {
    }

    public InstallSnapshot(RaftEndpoint leader, int term, SnapshotEntry snapshot) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public SnapshotEntry snapshot() {
        return snapshot;
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" + "leader=" + leader + ", term=" + term + ", snapshot=" + snapshot + '}';
    }

}
