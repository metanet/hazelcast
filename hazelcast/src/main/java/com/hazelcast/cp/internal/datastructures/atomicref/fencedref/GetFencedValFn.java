package com.hazelcast.cp.internal.datastructures.atomicref.fencedref;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class GetFencedValFn implements IFunction<FencedVal, Data>, IdentifiedDataSerializable {

    public GetFencedValFn() {
    }

    @Override
    public Data apply(FencedVal val) {
        return val != null ? val.getValue() : null;
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.GET_FENCED_VAL_FN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

}
