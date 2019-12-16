package com.hazelcast.cp.internal.datastructures.atomicref.fencedref;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

public class CASFencedValFn implements IFunction<FencedVal, FencedVal>, IdentifiedDataSerializable {

    private Data expectedValue;

    private Data newValue;

    private long newFence;

    public CASFencedValFn() {
    }

    public CASFencedValFn(Data expectedValue, Data newValue, long newFence) {
        this.expectedValue = expectedValue;
        this.newValue = newValue;
        this.newFence = newFence;
    }

    @Override
    public FencedVal apply(FencedVal val) {
        if (val == null) {
            return expectedValue != null ? val : new FencedVal(newValue, newFence);
        }

        if (newFence < 0 || val.getFence() > newFence || !Objects.equals(val.getValue(), expectedValue)) {
            return val;
        }

        return new FencedVal(newValue, newFence);
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.CAS_FENCED_VAL_FN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, expectedValue);
        IOUtil.writeData(out, newValue);
        out.writeLong(newFence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        expectedValue = IOUtil.readData(in);
        newValue = IOUtil.readData(in);
        newFence = in.readLong();
    }
}
