package com.hazelcast.internal.partition.impl;

import com.hazelcast.nio.Address;

interface MigrationRunnable extends Runnable {

    void invalidate(Address address);

    void invalidate(int partitionId);

    boolean isValid();
    
    boolean isPauseable();

}
