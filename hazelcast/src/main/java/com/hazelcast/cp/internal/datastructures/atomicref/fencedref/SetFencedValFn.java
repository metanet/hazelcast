package com.hazelcast.cp.internal.datastructures.atomicref.fencedref;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class SetFencedValFn implements IFunction<FencedVal, FencedVal>, IdentifiedDataSerializable {

    private Data newValue;

    private long newFence;

    public SetFencedValFn() {
    }

    public SetFencedValFn(Data newValue, long newFence) {
        this.newValue = newValue;
        this.newFence = newFence;
    }

    @Override
    public FencedVal apply(FencedVal val) {
        return (val == null || val.getFence() < newFence) ?  new FencedVal(newValue, newFence) : val;
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.SET_FENCED_VAL_FN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, newValue);
        out.writeLong(newFence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        newValue = IOUtil.readData(in);
        newFence = in.readLong();
    }

}
