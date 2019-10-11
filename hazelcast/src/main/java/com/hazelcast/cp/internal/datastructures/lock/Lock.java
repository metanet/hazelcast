/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState.NOT_LOCKED;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.min;

/**
 * State-machine implementation of the Raft-based lock
 */
public class Lock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    /**
     * Max number of reentrant lock acquires
     */
    private int lockCountLimit;

    /**
     * Current owner of the lock
     */
    private LockInvocationKey owner;

    /**
     * Number of acquires the current lock owner has committed
     */
    private int lockCount;

    /**
     * Responses of completed invocations for each lock endpoint. Used for
     * preventing duplicate execution of lock() / unlock() calls.
     */
    private Map<LockEndpoint, LockEndpointState> endpointStates = new HashMap<>();

    Lock() {
    }

    Lock(CPGroupId groupId, String name, int lockCountLimit) {
        super(groupId, name);
        this.lockCountLimit = lockCountLimit > 0 ? lockCountLimit : Integer.MAX_VALUE;
    }

    /**
     * Assigns the lock to the given endpoint, if the lock is currently
     * available. The lock count is incremented if the endpoint already holds
     * the lock. If the lock is assigned to some other endpoint and the caller
     * can wait, i.e., the "wait" argument is true, a wait key is added to
     * the wait queue for the caller. The lock count is not incremented if
     * the lock call is a retry. If the lock call is a retry of a wait key that
     * resides in the wait queue with the same invocation uid, the wait key is
     * updated to store the new call id. If the lock call is a new call of a
     * lock endpoint that resides in the wait queue with a different invocation
     * uid, the existing wait key is cancelled because it means the caller has
     * stopped waiting for response of the previous invocation.
     * <p>
     * Deduplication is applied to prevent duplicate execution of retried
     * invocations.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    AcquireResult acquire(LockInvocationKey key, boolean wait) {
        LockEndpoint endpoint = key.endpoint();
        UUID invocationUid = key.invocationUid();
        long callId = key.callId();
        LockEndpointState endpointState = getOrInitLockEndpointState(endpoint);
        LockOwnershipState response = endpointState.getMemoizedResponse(endpoint, invocationUid, callId);
        if (response != null) {
            AcquireStatus status = response.isLocked() ? SUCCESSFUL : FAILED;
            return new AcquireResult(status, response.getFence(), null);
        }

        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            if (lockCount == lockCountLimit) {
                endpointState.memoize(invocationUid, callId, NOT_LOCKED);
                return AcquireResult.failed(null);
            }

            lockCount++;
            endpointState.memoize(invocationUid, callId, lockOwnershipState());
            return AcquireResult.acquired(owner.commitIndex());
        }

        // We must cancel waits keys of previous invocation of the endpoint
        // before adding a new wait key or even if we will not wait.
        // If this is a stale request with an old call id,
        // we will fail inside the cancelWaitKey() call...
        LockInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, invocationUid, callId);

        if (wait) {
            addWaitKey(endpoint, key);
            return AcquireResult.waitKeyAdded(cancelledWaitKey);
        }

        endpointState.memoize(invocationUid, callId, NOT_LOCKED);
        return AcquireResult.failed(cancelledWaitKey);
    }

    private LockEndpointState getOrInitLockEndpointState(LockEndpoint endpoint) {
        return endpointStates.computeIfAbsent(endpoint, e -> new LockEndpointState());
    }

    private LockInvocationKey cancelWaitKey(LockEndpoint endpoint, UUID invocationUid, long callId) {
        LockInvocationKey key = getWaitKey(endpoint);
        if (key != null && key.isOlderInvocationOf(endpoint, invocationUid, callId)) {
            removeWaitKey(endpoint);
            return key;
        }

        return null;
    }

    /**
     * Releases the lock. The lock is freed when the lock count reaches to 0.
     * If the remaining lock count > 0 after a successful release, the lock is
     * still held by the endpoint. If the lock is assigned to some other
     * endpoint afterwards, the wait key of the new lock holder is returned for
     * sending the response. If the release call fails because the lock
     * endpoint is not the current lock holder, the caller endpoint might have
     * a wait key in the wait queue. If this is the case, its wait key is
     * completed with {@link WaitKeyCancelledException}.
     * <p>
     * Deduplication is applied to prevent duplicate execution of retried
     * invocations.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    ReleaseResult release(LockInvocationKey key) {
        return doRelease(key.endpoint(), key.invocationUid(), key.callId(), 1);
    }

    private ReleaseResult doRelease(LockEndpoint endpoint, UUID invocationUid, long callId, int releaseCount) {
        LockEndpointState endpointState = getOrInitLockEndpointState(endpoint);
        LockOwnershipState response = endpointState.getMemoizedResponse(endpoint, invocationUid, callId);
        if (response != null) {
            return ReleaseResult.successful(response);
        }

        if (owner == null || !owner.endpoint().equals(endpoint)) {
            LockInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, invocationUid, callId);
            endpointState.memoize(invocationUid, callId, NOT_LOCKED);
            return ReleaseResult.failed(cancelledWaitKey);
        }

        lockCount = lockCount - min(lockCount, releaseCount);
        if (lockCount > 0) {
            LockOwnershipState ownership = lockOwnershipState();
            endpointState.memoize(invocationUid, callId, ownership);
            return ReleaseResult.successful(ownership);
        }

        LockOwnershipState ownership = setNewLockOwner();

        endpointState.reset();
        endpointState.memoize(invocationUid, callId, ownership);

        return ReleaseResult.successful(ownership, owner);
    }

    private LockOwnershipState setNewLockOwner() {
        Iterator<LockInvocationKey> iter = waitKeyIterator();
        if (iter.hasNext()) {
            LockInvocationKey newOwner = iter.next();
            iter.remove();
            owner = newOwner;
            lockCount = 1;
            getOrInitLockEndpointState(owner.endpoint()).memoize(owner.invocationUid(), owner.callId(), lockOwnershipState());
        } else {
            owner = null;
        }

        return lockOwnershipState();
    }

    LockOwnershipState lockOwnershipState() {
        if (owner == null) {
            return LockOwnershipState.NOT_LOCKED;
        }

        return new LockOwnershipState(owner.commitIndex(), lockCount, owner.sessionId(), owner.endpoint().threadId());
    }

    Lock cloneForSnapshot() {
        Lock clone = new Lock();
        cloneForSnapshot(clone);
        clone.lockCountLimit = this.lockCountLimit;
        clone.owner = this.owner;
        clone.lockCount = this.lockCount;
        for (Entry<LockEndpoint, LockEndpointState> e : this.endpointStates.entrySet()) {
            clone.endpointStates.put(e.getKey(), e.getValue().cloneForSnapshot());
        }

        return clone;
    }

    /**
     * Releases the lock if the current lock holder's session is closed.
     * The lock will be assigned to the first wait key in the wait queue.
     */
    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
        removeLockEndpointStates(sessionId);

        if (owner != null && owner.sessionId() == sessionId) {
            ReleaseResult result = doRelease(owner.endpoint(), newUnsecureUUID(), Long.MAX_VALUE, lockCount);
            if (result.completedWaitKey() != null) {
                responses.put(result.completedWaitKey().commitIndex(), result.ownership().getFence());
            }
        }
    }

    private void removeLockEndpointStates(long sessionId) {
        endpointStates.keySet().removeIf(endpoint -> endpoint.sessionId() == sessionId);
    }

    /**
     * Returns session id of the current lock holder or an empty collection if
     * the lock is not held
     */
    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.emptyList();
    }

    @Override
    protected void onWaitKeyExpire(LockInvocationKey key) {
        getOrInitLockEndpointState(key.endpoint()).memoize(key.invocationUid(), key.callId(), NOT_LOCKED);
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.RAFT_LOCK;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(lockCountLimit);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        out.writeInt(endpointStates.size());
        for (Entry<LockEndpoint, LockEndpointState> e1 : endpointStates.entrySet()) {
            out.writeObject(e1.getKey());
            LockEndpointState endpointState = e1.getValue();
            out.writeLong(endpointState.greatestCallId);
            out.writeInt(endpointState.invocations.size());
            for (Entry<UUID, LockOwnershipState> e2 : endpointState.invocations.entrySet()) {
                writeUUID(out, e2.getKey());
                out.writeObject(e2.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        lockCountLimit = in.readInt();
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        int endpointStateCount = in.readInt();
        for (int i = 0; i < endpointStateCount; i++) {
            LockEndpoint endpoint = in.readObject();
            LockEndpointState endpointState = new LockEndpointState();

            endpointState.greatestCallId = in.readLong();
            int invocationCount = in.readInt();
            for (int j = 0; j < invocationCount; j++) {
                UUID invocationUid = readUUID(in);
                LockOwnershipState ownership = in.readObject();
                endpointState.invocations.put(invocationUid, ownership);
            }

            endpointStates.put(endpoint, endpointState);
        }
    }

    @Override
    public String toString() {
        return "Lock{" + internalToString() + ", lockCountLimit=" + lockCountLimit + ", owner="
                + owner + ", lockCount=" + lockCount + ", endpointStates=" + endpointStates + '}';
    }

    private static class LockEndpointState {

        /**
         * Responses of completed lock() and unlock() calls. If a lock
         * call is currently in the wait queue, its UUID is not in this map.
         */
        final Map<UUID, LockOwnershipState> invocations = new HashMap<>();

        /**
         * The greatest call id of completed invocations of the lock endpoint.
         */
        long greatestCallId = -1;

        LockEndpointState cloneForSnapshot() {
            LockEndpointState clone = new LockEndpointState();
            clone.greatestCallId = this.greatestCallId;
            clone.invocations.putAll(this.invocations);

            return clone;
        }

        LockOwnershipState getMemoizedResponse(LockEndpoint endpoint, UUID invocationUid, long callId) {
            LockOwnershipState response = invocations.get(invocationUid);
            if (response != null) {
                return response;
            } else if (callId < this.greatestCallId) {
                // We have already seen that the lock endpoint has made a more
                // recent invocation after the given one. It means that
                // the given operation is not valid anymore and we don't need
                // to process it.
                throw new WaitKeyCancelledException("Invocation: " + invocationUid + " with call id: " + callId
                        + " cannot be processed because a more recent invocation of " + endpoint + " has been already processed");
            }

            return null;
        }

        void memoize(UUID invocationUid, long callId, LockOwnershipState lockOwnership) {
            invocations.put(invocationUid, lockOwnership);
            this.greatestCallId = Math.max(this.greatestCallId, callId);
        }

        void reset() {
            invocations.clear();
        }

        @Override
        public String toString() {
            return "LockEndpointState{" + "greatestCallId=" + greatestCallId + ", invocations=" + invocations + '}';
        }
    }

}
