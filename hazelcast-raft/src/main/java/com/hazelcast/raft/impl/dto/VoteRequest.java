package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by candidates to gather votes (§5.2).
 */
public class VoteRequest {

    private RaftEndpoint candidate;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;

    public VoteRequest() {
    }

    public VoteRequest(RaftEndpoint candidate, int term, int lastLogTerm, long lastLogIndex) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint candidate() {
        return candidate;
    }

    public int term() {
        return term;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "VoteRequest{" + "candidate=" + candidate + ", term=" + term + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
