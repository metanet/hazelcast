package com.hazelcast.cp;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IFunction;

public interface IAtomicFencedReference<E> extends DistributedObject {

    E get();

    boolean attemptFence(long fence);

    boolean set(E newValue, long fence);

    boolean compareAndSet(E expectedValue, E newValue, long fence);

    boolean isNull();

    boolean clear(long fence);

    boolean contains(E value);

    void alter(IFunction<E, E> function, long fence);

    E alterAndGet(IFunction<E, E> function, long fence);

    E getAndAlter(IFunction<E, E> function);

    <R> R apply(IFunction<E, R> function);

}
