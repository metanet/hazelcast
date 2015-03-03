package com.hazelcast.core;

/**
 * PartitionLostListener provides the ability to be notified upon a possible data loss when a partition has no owner and backups.
 *
 * @see Partition
 * @see PartitionService
 *
 * @since 3.5
 */
public interface PartitionLostListener
        extends PartitionEventListener<PartitionLostEvent> {

    /**
     * Invoked when a partition loses its owner and all backups.
     *
     * @param event the event that contains the partition id and the replica index that has been lost
     */
    void onEvent(PartitionLostEvent event);

}
