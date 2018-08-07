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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphore extends BlockingResource<SemaphoreInvocationKey> implements IdentifiedDataSerializable {

    private boolean initialized;
    private int available;
    private final Map<Long, SessionState> sessionStates = new HashMap<Long, SessionState>();

    public RaftSemaphore() {
    }

    RaftSemaphore(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
        SessionState state = sessionStates.get(sessionId);
        if (state != null) {
            for (Entry<UUID, Integer> e : state.acquires.entrySet()) {
                UUID invocationUid = e.getKey();
                if (!state.releases.containsKey(invocationUid)) {
                    Collection<SemaphoreInvocationKey> keys = release(sessionId, invocationUid, e.getValue());
                    for (SemaphoreInvocationKey key : keys) {
                        result.put(key.commitIndex(), Boolean.TRUE);
                    }
                }
            }
        }
    }

    boolean init(int permits) {
        if (initialized || available != 0) {
            return false;
        }

        available = permits;
        initialized = true;

        return true;
    }

    int getAvailable() {
        return available;
    }

    boolean isAvailable(int permits) {
        checkPositive(permits, "Permits should be positive!");
        return available >= permits;
    }

    boolean acquire(SemaphoreInvocationKey key, int permits, boolean wait) {
        if (!isAvailable(permits)) {
            if (wait) {
                waitKeys.add(key);
            }

            return false;
        }

        doAcquire(key.sessionId(), key.invocationUid(), permits);

        return true;
    }

    private void doAcquire(long sessionId, UUID invocationUid, int permits) {
        if (sessionId == NO_SESSION_ID) {
            available -= permits;
            return;
        }

        SessionState state = sessionStates.get(sessionId);
        if (state == null) {
            state = new SessionState();
            sessionStates.put(sessionId, state);
        }

        if (state.acquires.put(invocationUid, permits) == null) {
            available -= permits;
        }
    }

    Collection<SemaphoreInvocationKey> release(long sessionId, UUID invocationUid, int permits) {
        checkPositive(permits, "Permits should be positive!");

        if (sessionId != NO_SESSION_ID) {
            SessionState state = sessionStates.get(sessionId);
            if (state == null) {
                throw new IllegalArgumentException("Cannot release " + permits
                        + " permits. Session has acquired no permits!");
            }

            if (state.releases.containsKey(invocationUid)) {
                return Collections.emptyList();
            }

            long acquired = state.getEffectivePermitCount();

            if (acquired < permits) {
                throw new IllegalArgumentException("Cannot release " + permits
                        + " permits. Session has acquired only " + acquired + " permits!");
            }

            state.releases.put(invocationUid, permits);
            if (state.getEffectivePermitCount() == 0) {
                sessionStates.remove(sessionId);
            }
        }

        available += permits;

        return assignPermitsToWaitKeys();
    }

    private Collection<SemaphoreInvocationKey> assignPermitsToWaitKeys() {
        List<SemaphoreInvocationKey> keys = new ArrayList<SemaphoreInvocationKey>();
        Iterator<SemaphoreInvocationKey> iterator = waitKeys.iterator();
        while (iterator.hasNext()) {
            SemaphoreInvocationKey key = iterator.next();
            if (key.permits() > available) {
                break;
            }

            iterator.remove();
            keys.add(key);
            doAcquire(key.sessionId(), key.invocationUid(), key.permits());
        }

        return keys;
    }

    int drain(long sessionId, UUID invocationUid) {
        SessionState state = sessionStates.get(sessionId);
        if (state != null) {
            Integer permits = state.acquires.get(invocationUid);
            if (permits != null) {
                return permits;
            }
        }

        int drained = available;
        if (drained > 0) {
            doAcquire(sessionId, invocationUid, drained);
        }
        available = 0;

        return drained;
    }

    @Override
    public Collection<Long> getOwnerSessionIds() {
        return unmodifiableCollection(sessionStates.keySet());
    }

    Tuple2<Boolean, Collection<SemaphoreInvocationKey>> change(int permits) {
        if (permits == 0) {
            Collection<SemaphoreInvocationKey> c = Collections.emptyList();
            return Tuple2.of(false, c);
        }

        available += permits;
        initialized = true;

        Collection<SemaphoreInvocationKey> keys =
                permits > 0 ? assignPermitsToWaitKeys() : Collections.<SemaphoreInvocationKey>emptyList();

        return Tuple2.of(true, keys);
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.RAFT_SEMAPHORE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeBoolean(initialized);
        out.writeInt(available);
        out.writeInt(sessionStates.size());
        for (Entry<Long, SessionState> e1 : sessionStates.entrySet()) {
            out.writeLong(e1.getKey());
            SessionState state = e1.getValue();
            out.writeInt(state.acquires.size());
            for (Entry<UUID, Integer> e2 : state.acquires.entrySet()) {
                UUID invocationUid = e2.getKey();
                int permits = e2.getValue();
                out.writeLong(invocationUid.getLeastSignificantBits());
                out.writeLong(invocationUid.getMostSignificantBits());
                out.writeInt(permits);
            }
            out.writeInt(state.releases.size());
            for (Entry<UUID, Integer> e2 : state.releases.entrySet()) {
                UUID invocationUid = e2.getKey();
                int permits = e2.getValue();
                out.writeLong(invocationUid.getLeastSignificantBits());
                out.writeLong(invocationUid.getMostSignificantBits());
                out.writeInt(permits);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        initialized = in.readBoolean();
        available = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            SessionState state = new SessionState();
            int acquireCount = in.readInt();
            for (int j = 0; j < acquireCount; j++) {
                long least = in.readLong();
                long most = in.readLong();
                int permits = in.readInt();
                state.acquires.put(new UUID(most, least), permits);
            }
            int releaseCount = in.readInt();
            for (int j = 0; j < releaseCount; j++) {
                long least = in.readLong();
                long most = in.readLong();
                int permits = in.readInt();
                state.releases.put(new UUID(most, least), permits);
            }

            sessionStates.put(sessionId, state);
        }
    }

    @Override
    public String toString() {
        return "RaftSemaphore{" + "groupId=" + groupId + ", name='" + name + '\'' + ", initialized=" + initialized
                + ", available=" + available + ", sessionStates=" + sessionStates + ", waitKeys=" + waitKeys + '}';
    }

    private static class SessionState {
        private final Map<UUID, Integer> acquires = new HashMap<UUID, Integer>();
        private final Map<UUID, Integer> releases = new HashMap<UUID, Integer>();

        int getEffectivePermitCount() {
            int count = 0;
            for (int p : acquires.values()) {
                count += p;
            }

            for (int p : releases.values()) {
                count -= p;
            }

            return count;
        }

        @Override
        public String toString() {
            return "SemaphoreState{" + "acquires=" + acquires + ", releases=" + releases + '}';
        }
    }

}
