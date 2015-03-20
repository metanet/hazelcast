package com.hazelcast.config;

import com.hazelcast.map.listener.MapPartitionLostListener;

/**
 * Configuration for MapPartitionLostListener
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostListenerConfig
        extends ListenerConfig {

    private MapPartitionLostListenerConfigReadOnly readOnly;

    public MapPartitionLostListenerConfig() {
        super();
    }

    public MapPartitionLostListenerConfig(String className) {
        super(className);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListener implementation) {
        super(implementation);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    public MapPartitionLostListenerConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapPartitionLostListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    public MapPartitionLostListenerConfig setImplementation(final MapPartitionLostListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

}
