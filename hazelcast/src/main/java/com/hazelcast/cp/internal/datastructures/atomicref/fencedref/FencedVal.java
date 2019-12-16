package com.hazelcast.cp.internal.datastructures.atomicref.fencedref;

import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class FencedVal implements IdentifiedDataSerializable {

    private Data value;

    private long fence;

    public FencedVal() {
    }

    public FencedVal(Data value, long fence) {
        this.value = value;
        this.fence = fence;
    }

    public Data getValue() {
        return value;
    }

    public long getFence() {
        return fence;
    }

    @Override
    public final int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.FENCED_VAL;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        IOUtil.writeData(out, value);
        out.writeLong(fence);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        value = IOUtil.readData(in);
        fence = in.readLong();
    }
}
