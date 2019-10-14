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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;

/**
 * State-machine implementation of the Raft-based semaphore.
 * Supports both sessionless and session-aware semaphore proxies.
 */
public class Semaphore extends BlockingResource<SemaphoreInvocationKey> implements IdentifiedDataSerializable {

    private boolean initialized;

    private int available;

    /**
     * Responses of completed invocations for each semaphore endpoint. Used for
     * preventing duplicate execution of acquire(), release(), drain() and
     * change() calls.
     */
    private final Long2ObjectHashMap<SessionState> sessionStates = new Long2ObjectHashMap<>();

    Semaphore() {
    }

    Semaphore(CPGroupId groupId, String name) {
        super(groupId, name);
    }

    Collection<SemaphoreInvocationKey> init(int permits) {
        if (initialized || available != 0) {
            throw new IllegalStateException("Semaphore already initialized!");
        }

        available = permits;
        initialized = true;

        return assignPermitsToWaitKeys();
    }

    int getAvailable() {
        return available;
    }

    boolean isAvailable(int permits) {
        checkPositive(permits, "Permits should be positive!");
        return available >= permits;
    }

    /**
     * Assigns permits to the given semaphore endpoint, if sufficient number of
     * permits are available. If there are no sufficient number of permits and
     * the caller can wait, i.e., the "wait" argument is true, a wait key is
     * created and added to the wait queue. Permits are not assigned if
     * the acquire() call is a retry of a completed acquire() call of
     * a session-aware proxy. Permits are assigned again if the acquire() call
     * is a retry of a successful acquire call of a sessionless proxy.
     * If the acquire() call is a retry of an endpoint that resides in the wait
     * queue with the same invocation uid, the wait key is updated with the new
     * call id. If the acquire() call is a new invocation of an endpoint that
     * resides in the wait queue with a different invocation uid, the existing
     * wait key is cancelled with {@link WaitKeyCancelledException} because it
     * means the caller has stopped waiting for response of the previous
     * invocation.
     * <p>
     * Deduplication is applied to prevent duplicate execution of retried
     * invocations.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    AcquireResult acquire(SemaphoreInvocationKey key, boolean wait) {
        SemaphoreEndpoint endpoint = key.endpoint();

        if (key.sessionId() != NO_SESSION_ID) {
            SessionState state = getOrInitSessionSemaphoreState(key.sessionId());
            Integer acquired = state.getMemoizedResponse(endpoint, key.invocationUid(), key.callId());
            if (acquired != null) {
                AcquireStatus status = acquired > 0 ? SUCCESSFUL : FAILED;
                return new AcquireResult(status, acquired, null);
            }
        }

        // If this is a stale request with an old call id,
        // we will fail inside the cancelWaitKey() call...
        SemaphoreInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, key.invocationUid(), key.callId());

        if (!isAvailable(key.permits())) {
            AcquireStatus status;
            if (wait) {
                addWaitKey(endpoint, key);
                status = WAIT_KEY_ADDED;
            } else {
                assignPermitsToInvocation(endpoint, key.invocationUid(), key.callId(), 0);
                status = FAILED;
            }

            return new AcquireResult(status, 0, cancelledWaitKey);
        }

        assignPermitsToInvocation(endpoint, key.invocationUid(), key.callId(), key.permits());

        return new AcquireResult(SUCCESSFUL, key.permits(), cancelledWaitKey);
    }

    private void assignPermitsToInvocation(SemaphoreEndpoint endpoint, UUID invocationUid, long callId, int permits) {
        long sessionId = endpoint.sessionId();
        if (sessionId == NO_SESSION_ID) {
            available -= permits;
            return;
        }

        SessionState sessionState = getOrInitSessionSemaphoreState(sessionId);
        sessionState.acquiredPermits += permits;
        available -= permits;
        sessionState.memoize(endpoint, invocationUid, callId, permits);
    }

    private SessionState getOrInitSessionSemaphoreState(long sessionId) {
        checkTrue(sessionId != NO_SESSION_ID, "Cannot memorize results for sessionless semaphore");
        return sessionStates.computeIfAbsent(sessionId, s -> new SessionState());
    }

    /**
     * Releases the given number of permits. Permits are not released if it is
     * a retry of a previous successful release() call of a session-aware
     * proxy. Permits are released again if it is a retry of a completed
     * release() call of a sessionless proxy. If the release() call fails
     * because the requesting endpoint does not hold the given number of
     * permits, existing wait key of the endpoint is cancelled because a new
     * release() call means that the endpoint has stopped waiting for response
     * of its previous acquire() call.
     * <p>
     * Returns completed wait keys, i.e., new permit holders, after
     * a successful release. Returns the cancelled wait key if there is any
     * after a failed release() call.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    ReleaseResult release(SemaphoreInvocationKey key) {
        return release(key.endpoint(), key.invocationUid(), key.callId(), key.permits());
    }

    private ReleaseResult release(SemaphoreEndpoint endpoint, UUID invocationUid, long callId, int permits) {
        checkPositive(permits, "Permits should be positive!");

        long sessionId = endpoint.sessionId();
        if (sessionId != NO_SESSION_ID) {
            SessionState sessionState = getOrInitSessionSemaphoreState(sessionId);
            Integer response = sessionState.getMemoizedResponse(endpoint, invocationUid, callId);
            if (response != null) {
                if (response > 0) {
                    return ReleaseResult.successful(Collections.emptyList(), null);
                } else {
                    return ReleaseResult.failed(null);
                }
            }

            if (sessionState.acquiredPermits < permits) {
                sessionState.memoize(endpoint, invocationUid, callId, 0);
                return ReleaseResult.failed(cancelWaitKey(endpoint, invocationUid, callId));
            }

            sessionState.acquiredPermits -= permits;
            sessionState.memoize(endpoint, invocationUid, callId, permits);
        }

        available += permits;

        // order is important...
        SemaphoreInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, invocationUid, callId);
        Collection<SemaphoreInvocationKey> acquiredWaitKeys = assignPermitsToWaitKeys();

        return ReleaseResult.successful(acquiredWaitKeys, cancelledWaitKey);
    }

    Semaphore cloneForSnapshot() {
        Semaphore clone = new Semaphore();
        cloneForSnapshot(clone);
        clone.initialized = this.initialized;
        clone.available = this.available;
        for (Entry<Long, SessionState> e : this.sessionStates.entrySet()) {
            SessionState s = new SessionState();
            s.acquiredPermits = e.getValue().acquiredPermits;
            s.invocations.putAll(e.getValue().invocations);
            clone.sessionStates.put(e.getKey(), s);
        }

        return clone;
    }

    private SemaphoreInvocationKey cancelWaitKey(SemaphoreEndpoint endpoint, UUID invocationUid, long callId) {
        SemaphoreInvocationKey key = getWaitKey(endpoint);
        if (key != null && key.isOlderInvocationOf(endpoint, invocationUid, callId)) {
            removeWaitKey(endpoint);
            return key;
        }

        return null;
    }

    private Collection<SemaphoreInvocationKey> assignPermitsToWaitKeys() {
        List<SemaphoreInvocationKey> acquiredWaitKeys = new ArrayList<>();
        Iterator<SemaphoreInvocationKey> iterator = waitKeyIterator();
        while (iterator.hasNext() && available > 0) {
            SemaphoreInvocationKey key = iterator.next();
            if (isAvailable(key.permits())) {
                iterator.remove();
                acquiredWaitKeys.add(key);
                assignPermitsToInvocation(key.endpoint(), key.invocationUid(), key.callId(), key.permits());
            }
        }

        return acquiredWaitKeys;
    }

    /**
     * Assigns all available permits to the given session endpoint. Permits are
     * not assigned if the drain() call is a retry of a completed drain() call
     * of a session-aware proxy. Permits are assigned again if the drain() call
     * is a retry of a completed drain() call of a sessionless proxy.
     * <p>
     * Returns cancelled wait key of the same endpoint if there are any.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    AcquireResult drain(SemaphoreInvocationKey key) {
        SemaphoreEndpoint endpoint = key.endpoint();
        UUID invocationUid = key.invocationUid();

        if (key.sessionId() != NO_SESSION_ID) {
            SessionState sessionState = getOrInitSessionSemaphoreState(endpoint.sessionId());
            Integer response = sessionState.getMemoizedResponse(endpoint, invocationUid, key.callId());
            if (response != null) {
                // Even if 0 permit is assigned, this is a successful operation
                return new AcquireResult(SUCCESSFUL, response, null);
            }
        }

        SemaphoreInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, invocationUid, key.callId());

        int drained = available;
        assignPermitsToInvocation(endpoint, invocationUid, key.callId(), drained);
        available = 0;

        // Even if 0 permit is assigned, this is a successful operation
        return new AcquireResult(SUCCESSFUL, drained, cancelledWaitKey);
    }

    /**
     * Changes the number of permits by adding the given value. Permits are not
     * changed if it is a retry of a completed change() call of a session-aware
     * proxy. Permits are changed again if it is a retry of a completed
     * change() call of a sessionless proxy. If number of permits increase, new
     * permit assignments can be done.
     * <p>
     * Returns completed wait keys after a successful change. Returns cancelled
     * wait key of the same endpoint if there is any.
     * <p>
     * If an invocation with a call id that is smaller than the greatest call
     * id of the given endpoint is received, it means that this invocation is
     * not valid anymore, thus it fails with {@link WaitKeyCancelledException}.
     */
    ReleaseResult change(SemaphoreInvocationKey key) {
        int permits = key.permits();
        if (permits == 0) {
            return ReleaseResult.failed(null);
        }

        SemaphoreEndpoint endpoint = key.endpoint();

        long sessionId = endpoint.sessionId();
        if (sessionId != NO_SESSION_ID) {
            SessionState state = getOrInitSessionSemaphoreState(sessionId);
            Integer response = state.getMemoizedResponse(endpoint, key.invocationUid(), key.callId());
            if (response != null) {
                return ReleaseResult.successful(Collections.emptyList(), null);
            }

            state.memoize(endpoint, key.invocationUid(), key.callId(), permits);
        }

        SemaphoreInvocationKey cancelledWaitKey = cancelWaitKey(endpoint, key.invocationUid(), key.callId());

        available += permits;
        initialized = true;

        Collection<SemaphoreInvocationKey> acquiredWaitKeys = permits > 0 ? assignPermitsToWaitKeys() : Collections.emptyList();
        return ReleaseResult.successful(acquiredWaitKeys, cancelledWaitKey);
    }

    /**
     * Releases permits of the closed session.
     */
    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
        SessionState state = sessionStates.get(sessionId);
        if (state != null) {
            // remove the session after release() because release() checks existence of the session
            if (state.acquiredPermits > 0) {
                SemaphoreEndpoint endpoint = new SemaphoreEndpoint(sessionId, 0);
                ReleaseResult result = release(endpoint, newUnsecureUUID(), Long.MAX_VALUE, state.acquiredPermits);
                // wait keys are already deleted...
                assert result.cancelledWaitKey() == null;
                for (SemaphoreInvocationKey key : result.acquiredWaitKeys()) {
                    responses.put(key.commitIndex(), Boolean.TRUE);
                }
            }

            sessionStates.remove(sessionId);
        }
    }

    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        Set<Long> activeSessionIds = new HashSet<>();
        for (Entry<Long, SessionState> e : sessionStates.entrySet()) {
            if (e.getValue().acquiredPermits > 0) {
                activeSessionIds.add(e.getKey());
            }
        }

        return activeSessionIds;
    }

    @Override
    protected void onWaitKeyExpire(SemaphoreInvocationKey key) {
        assignPermitsToInvocation(key.endpoint(), key.invocationUid(), key.callId(), 0);
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.RAFT_SEMAPHORE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(initialized);
        out.writeInt(available);
        out.writeInt(sessionStates.size());
        for (Entry<Long, SessionState> e1 : sessionStates.entrySet()) {
            out.writeLong(e1.getKey());
            SessionState state = e1.getValue();
            out.writeInt(state.invocations.size());
            for (Entry<Long, TriTuple<UUID, Long, Integer>> e2 : state.invocations.entrySet()) {
                out.writeLong(e2.getKey());
                TriTuple<UUID, Long, Integer> t = e2.getValue();
                writeUUID(out, t.element1);
                out.writeLong(t.element2);
                out.writeInt(t.element3);
            }

            out.writeInt(state.acquiredPermits);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        initialized = in.readBoolean();
        available = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            SessionState state = new SessionState();
            int invocationCount = in.readInt();
            for (int j = 0; j < invocationCount; j++) {
                long threadId = in.readLong();
                UUID invocationUid = readUUID(in);
                long callId = in.readLong();
                int permits = in.readInt();
                state.invocations.put(threadId, TriTuple.of(invocationUid, callId, permits));
            }

            state.acquiredPermits = in.readInt();

            sessionStates.put(sessionId, state);
        }
    }

    @Override
    public String toString() {
        return "Semaphore{" + internalToString() + ", initialized=" + initialized
                + ", available=" + available + ", sessionStates=" + sessionStates + '}';
    }

    private static class SessionState {

        /**
         * Responses of the last acquire(), release(), drain() or change()
         * call for each thread. If an acquire() call is currently in the wait
         * queue, its UUID is not in this map.
         */
        final Long2ObjectHashMap<TriTuple<UUID, Long, Integer>> invocations = new Long2ObjectHashMap<>();

        /**
         * The greatest call id of completed invocations of each thread.
         */
//        final Long2LongHashMap greatestCallIds = new Long2LongHashMap(-1L);

        int acquiredPermits;

        Integer getMemoizedResponse(SemaphoreEndpoint endpoint, UUID invocationUid, long callId) {
            TriTuple<UUID, Long, Integer> t = invocations.get(endpoint.threadId());
            if (t != null) {
                if (t.element1.equals(invocationUid)) {
                    return t.element3;
                }

                if (t.element2 >= callId) {
                    // We have already seen that the semaphore endpoint has made
                    // a more recent invocation after the given one in the given
                    // thread. It means that the given operation is not valid
                    // anymore and we don't need to process it.
                    throw new WaitKeyCancelledException("Invocation: " + invocationUid + " with call id: " + callId
                            + " cannot be processed because a more recent invocation of " + endpoint + " has been already processed");
                }
            }

            return null;
        }

        void memoize(SemaphoreEndpoint endpoint, UUID invocationUid, long callId, int permits) {
            TriTuple<UUID, Long, Integer> t = invocations.get(endpoint.threadId());
            if (t != null && t.element2 >= callId) {
                throw new IllegalArgumentException("invalid call id: " + callId + " greatest call id: " + t.element2);
            }

            invocations.put(endpoint.threadId(), TriTuple.of(invocationUid, callId, permits));
        }

        @Override
        public String toString() {
            return "SessionState{" + "invocations=" + invocations + ", acquiredPermits=" + acquiredPermits + '}';
        }
    }

}
