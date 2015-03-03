package com.hazelcast.map.impl;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapPartitionLostEvent;
import com.hazelcast.map.listener.MapPartitionLostListener;

class InternalMapPartitionLostListenerAdapter
        implements ListenerAdapter {

    private final MapPartitionLostListener partitionLostListener;

    public InternalMapPartitionLostListenerAdapter(MapPartitionLostListener partitionLostListener) {
        this.partitionLostListener = partitionLostListener;
    }

    @Override
    public void onEvent(IMapEvent event) {
        partitionLostListener.partitionLost((MapPartitionLostEvent) event);
    }

}
