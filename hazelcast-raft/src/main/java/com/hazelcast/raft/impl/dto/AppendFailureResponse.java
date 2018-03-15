package com.hazelcast.raft.impl.dto;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * Struct for failure response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 */
public class AppendFailureResponse {

    private RaftEndpoint follower;
    private int term;
    private long expectedNextIndex;

    public AppendFailureResponse() {
    }

    public AppendFailureResponse(RaftEndpoint follower, int term, long expectedNextIndex) {
        this.follower = follower;
        this.term = term;
        this.expectedNextIndex = expectedNextIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long expectedNextIndex() {
        return expectedNextIndex;
    }

    @Override
    public String toString() {
        return "AppendFailureResponse{" + "follower=" + follower + ", term=" + term + ", expectedNextIndex="
                + expectedNextIndex + '}';
    }

}
