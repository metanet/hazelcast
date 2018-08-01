/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.ResourceRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;

/**
 * TODO: Javadoc Pending...
 */
class LockRegistry extends ResourceRegistry<LockInvocationKey, RaftLock> implements IdentifiedDataSerializable {

    public LockRegistry() {
    }

    LockRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftLock createNewResource(RaftGroupId groupId, String name) {
        return new RaftLock(groupId, name);
    }

    long acquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        return getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, true);
    }

    long tryAcquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid, long timeoutMs) {
        boolean wait = (timeoutMs > 0);
        long fence = getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, wait);
        if (wait && fence == INVALID_FENCE) {
            addWaitKey(new LockInvocationKey(name, endpoint, commitIndex, invocationUid), timeoutMs);
        }

        return fence;
    }

    Collection<LockInvocationKey> release(String name, LockEndpoint endpoint, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> keys = lock.release(endpoint, invocationUid);
        for (LockInvocationKey key : keys) {
            removeWaitKey(key);
        }

        return keys;
    }

    Collection<LockInvocationKey> forceRelease(String name, long expectedFence, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> keys = lock.forceRelease(expectedFence, invocationUid);
        for (LockInvocationKey key : keys) {
            removeWaitKey(key);
        }

        return keys;
    }

    int getLockCount(String name, LockEndpoint endpoint) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return 0;
        }

        if (endpoint != null) {
            LockInvocationKey owner = lock.owner();
            return (owner != null && endpoint.equals(owner.endpoint())) ? lock.lockCount() : 0;
        }

        return lock.lockCount();
    }

    long getLockFence(String name, LockEndpoint endpoint) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            throw new IllegalMonitorStateException();
        }

        LockInvocationKey owner = lock.owner();
        if (owner == null) {
            throw new IllegalMonitorStateException("Lock[" + name + "] has no owner!");
        } else if (endpoint != null && !owner.endpoint().equals(endpoint)) {
            throw new IllegalMonitorStateException("Lock[" + name + "] is owned by " + owner.endpoint() + "!");
        }

        return owner.commitIndex();
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_REGISTRY;
    }
}
