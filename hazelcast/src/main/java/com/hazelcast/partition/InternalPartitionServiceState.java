package com.hazelcast.partition;

import com.hazelcast.spi.annotation.PrivateApi;

@PrivateApi
public enum InternalPartitionServiceState {

    OK,
    MIGRATION_LOCAL,
    MIGRATION_ON_MASTER,
    REPLICA_NOT_SYNC

}
