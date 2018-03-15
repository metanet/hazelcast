package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i> by Henrik Ingo.
 *
 * @see VoteRequest
 */
public class PreVoteRequest {

    private RaftEndpoint candidate;
    private int nextTerm;
    private int lastLogTerm;
    private long lastLogIndex;

    public PreVoteRequest() {
    }

    public PreVoteRequest(RaftEndpoint candidate, int nextTerm, int lastLogTerm, long lastLogIndex) {
        this.nextTerm = nextTerm;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint candidate() {
        return candidate;
    }

    public int nextTerm() {
        return nextTerm;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "PreVoteRequest{" + "candidate=" + candidate + ", nextTerm=" + nextTerm + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
