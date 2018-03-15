package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for successful response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendFailureResponse
 */
public class AppendSuccessResponse {

    private RaftEndpoint follower;
    private int term;
    private long lastLogIndex;

    public AppendSuccessResponse() {
    }

    public AppendSuccessResponse(RaftEndpoint follower, int term, long lastLogIndex) {
        this.follower = follower;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "AppendSuccessResponse{" + "follower=" + follower + ", term=" + term  + ", lastLogIndex="
                + lastLogIndex + '}';
    }

}
