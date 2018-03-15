package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for response to PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i> by Henrik Ingo.
 *
 * @see PreVoteRequest
 * @see VoteResponse
 */
public class PreVoteResponse {

    private RaftEndpoint voter;
    private int term;
    private boolean granted;

    public PreVoteResponse() {
    }

    public PreVoteResponse(RaftEndpoint voter, int term, boolean granted) {
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
        return "PreVoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
