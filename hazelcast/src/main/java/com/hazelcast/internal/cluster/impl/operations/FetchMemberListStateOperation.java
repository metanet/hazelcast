package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;

public class FetchMemberListStateOperation extends AbstractClusterOperation implements JoinOperation {

    public FetchMemberListStateOperation() {
    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE_OPERATION;
    }

}
