package com.hazelcast.map.listener;

import com.hazelcast.core.MapPartitionLostEvent;

/**
 * Invoked when owner and all backups of a partition is lost for a specific map
 *
 * @since 3.5
 */
public interface MapPartitionLostListener extends MapListener {

    /**
     * Invoked when owner and all backups of a partition is lost for a specific map
     *
     * @param event the event object that contains map name and lost partition id
     */
    void partitionLost(MapPartitionLostEvent event);

}
