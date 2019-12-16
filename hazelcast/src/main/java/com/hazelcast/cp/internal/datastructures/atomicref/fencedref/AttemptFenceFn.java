package com.hazelcast.cp.internal.datastructures.atomicref.fencedref;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class AttemptFenceFn implements IFunction<FencedVal, FencedVal>, IdentifiedDataSerializable {

    long newFence;

    public AttemptFenceFn() {
    }

    public AttemptFenceFn(long newFence) {
        this.newFence = newFence;
    }

    @Override
    public FencedVal apply(FencedVal val) {
        if (newFence < 0 || (val != null && val.getFence() > newFence)) {
            return val;
        }

        return new FencedVal(val != null ? val.getValue() : null, newFence);
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.ATTEMPT_FENCE_FN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(newFence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        newFence = in.readLong();
    }
}
