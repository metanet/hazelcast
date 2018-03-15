package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.LogEntry;

import java.util.Arrays;

/**
 * Struct for AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
 */
public class AppendRequest {

    private RaftEndpoint leader;
    private int term;
    private int prevLogTerm;
    private long prevLogIndex;
    private long leaderCommitIndex;
    private LogEntry[] entries;

    public AppendRequest() {
    }

    public AppendRequest(RaftEndpoint leader, int term, int prevLogTerm, long prevLogIndex, long leaderCommitIndex,
                         LogEntry[] entries) {
        this.term = term;
        this.leader = leader;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = entries;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public int prevLogTerm() {
        return prevLogTerm;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public long leaderCommitIndex() {
        return leaderCommitIndex;
    }

    public LogEntry[] entries() {
        return entries;
    }

    public int entryCount() {
        return entries.length;
    }

    @Override
    public String toString() {
        return "AppendRequest{" + "leader=" + leader + ", term=" + term + ", prevLogTerm=" + prevLogTerm
                + ", prevLogIndex=" + prevLogIndex + ", leaderCommitIndex=" + leaderCommitIndex + ", entries=" + Arrays
                .toString(entries) + '}';
    }

}
