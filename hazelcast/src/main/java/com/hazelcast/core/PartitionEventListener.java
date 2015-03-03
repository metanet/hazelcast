package com.hazelcast.core;

import java.util.EventListener;

/**
 * PartitionEventListener is an interface for partition-related event listeners
 *
 * @param <T> A partition-related event class
 * @see MigrationListener
 * @see PartitionLostListener
 */
public interface PartitionEventListener<T extends PartitionEvent>
        extends EventListener {

    void onEvent(T event);

}
