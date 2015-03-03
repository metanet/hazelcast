package com.hazelcast.core;

/**
 * PartitionEvent is an interface for partition-related events
 * @see MigrationEvent
 * @see PartitionLostEvent
 */
public interface PartitionEvent {

    int getPartitionId();

}
