package com.hazelcast.cp.internal.datastructures.atomicref.proxy;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicFencedReference;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.AttemptFenceFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.CASFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.FencedVal;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.GetFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.fencedref.SetFencedValFn;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Objects;

import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_NEW_VALUE;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

public class AtomicFencedRefProxy<T> implements IAtomicFencedReference<T> {

    private final RaftInvocationManager invocationManager;
    private final SerializationService serializationService;
    private final RaftGroupId groupId;
    private final String proxyName;
    private final String objectName;

    public AtomicFencedRefProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.serializationService = nodeEngine.getSerializationService();
        this.groupId = groupId;
        this.proxyName = proxyName;
        this.objectName = objectName;
    }


    @Override
    public T get() {
        RaftOp op = new ApplyOp(objectName, toData(new GetFencedValFn()), RETURN_NEW_VALUE, false);
        return invocationManager.<T>query(groupId, op, LINEARIZABLE).joinInternal();
    }

    @Override
    public boolean attemptFence(long fence) {
        checkNotNegative(fence, "invalid fence!");

        RaftOp op = new ApplyOp(objectName, toData(new AttemptFenceFn(fence)), RETURN_NEW_VALUE, true);
        FencedVal val = invocationManager.<FencedVal>invoke(groupId, op).joinInternal();
        return val != null && val.getFence() == fence;
    }

    @Override
    public boolean set(T newValue, long fence) {
        checkNotNegative(fence, "invalid fence!");

        Data data = toData(newValue);
        RaftOp op = new ApplyOp(objectName, toData(new SetFencedValFn(data, fence)), RETURN_NEW_VALUE, true);
        FencedVal val = invocationManager.<FencedVal>invoke(groupId, op).joinInternal();
        return val != null && val.getFence() == fence && Objects.equals(data, val.getValue());
    }

    @Override
    public boolean compareAndSet(T expectedValue, T newValue, long fence) {
        checkNotNegative(fence, "invalid fence!");

        Data expectedData = toData(expectedValue);
        Data newData = toData(newValue);
        RaftOp op = new ApplyOp(objectName, toData(new CASFencedValFn(expectedData, newData, fence)), RETURN_NEW_VALUE, true);
        FencedVal val = invocationManager.<FencedVal>invoke(groupId, op).joinInternal();
        return val != null && val.getFence() == fence && Objects.equals(newData, val.getValue());
    }

    @Override
    public boolean isNull() {
        return get() == null;
    }

    @Override
    public boolean clear(long fence) {
        checkNotNegative(fence, "invalid fence!");

        RaftOp op = new ApplyOp(objectName, toData(new SetFencedValFn(null, fence)), RETURN_NEW_VALUE, true);
        FencedVal val = invocationManager.<FencedVal>invoke(groupId, op).joinInternal();
        return val != null && val.getFence() == fence && val.getValue() == null;
    }

    @Override
    public boolean contains(T value) {
        return Objects.equals(get(), value);
    }

    @Override
    public void alter(IFunction<T, T> function, long fence) {

    }

    @Override
    public T alterAndGet(IFunction<T, T> function, long fence) {
        return null;
    }

    @Override
    public T getAndAlter(IFunction<T, T> function) {
        return null;
    }

    @Override
    public <R> R apply(IFunction<T, R> function) {
        return null;
    }

    @Override
    public String getPartitionKey() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public void destroy() {

    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }
}
