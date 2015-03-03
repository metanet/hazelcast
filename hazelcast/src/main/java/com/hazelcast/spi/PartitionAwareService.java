package com.hazelcast.spi;

import com.hazelcast.partition.InternalPartitionLostEvent;

/**
 * An interface that can be implemented by SPI services to get notified of when a partition-related event occurs; for example, if a
 * {@link com.hazelcast.map.impl.MapService} notifies its map listeners when partition is lost for a map.
 */
public interface PartitionAwareService {

    void onPartitionLostEvent(InternalPartitionLostEvent event);

}
