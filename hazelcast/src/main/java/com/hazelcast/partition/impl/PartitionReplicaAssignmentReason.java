package com.hazelcast.partition.impl;

enum PartitionReplicaAssignmentReason {

    /*
        Used for initial partition assignments and migrations
     */
    ASSIGNMENT,

    MEMBER_REMOVED

}
