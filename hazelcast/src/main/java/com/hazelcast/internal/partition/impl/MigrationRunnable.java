package com.hazelcast.internal.partition.impl;

public interface MigrationRunnable extends Runnable {

    void invalidate();

    boolean isValid();

}
