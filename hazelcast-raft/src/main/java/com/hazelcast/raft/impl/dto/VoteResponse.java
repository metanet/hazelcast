package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for response to VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteRequest
 */
public class VoteResponse {

    private RaftEndpoint voter;
    private int term;
    private boolean granted;

    public VoteResponse() {
    }

    public VoteResponse(RaftEndpoint voter, int term, boolean granted) {
        this.voter = voter;
        this.term = term;
        this.granted = granted;
    }

    public RaftEndpoint voter() {
        return voter;
    }

    public int term() {
        return term;
    }

    public boolean granted() {
        return granted;
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
