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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock {

    private final RaftGroupId groupId;
    private final String name;
    private LockOwner owner;
    private LinkedList<LockInvocationKey> waiters = new LinkedList<LockInvocationKey>();

    RaftLock(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftLock(RaftLockSnapshot snapshot) {
        this.groupId = snapshot.getGroupId();
        this.name = snapshot.getName();
        this.owner = snapshot.getOwner();
        this.waiters.addAll(snapshot.getWaiters());
    }

    boolean acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        if (owner == null) {
            owner = new LockOwner(endpoint);
        }

        if (owner.endpoint.equals(endpoint)) {
            owner.acquire(invocationUid);
            return true;
        }

        if (wait) {
            waiters.offer(new LockInvocationKey(name, endpoint, invocationUid, commitIndex));
        }

        return false;
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        if (owner != null && endpoint.equals(owner.endpoint)) {
            owner.release(invocationUid, releaseCount);

            if (owner.lockCount() > 0) {
                return Collections.emptyList();
            }

            LockInvocationKey next = waiters.poll();
            if (next != null) {
                List<LockInvocationKey> entries = new ArrayList<LockInvocationKey>();
                entries.add(next);

                Iterator<LockInvocationKey> iter = waiters.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey n = iter.next();
                    if (next.invocationUid().equals(n.invocationUid())) {
                        iter.remove();
                        assert next.endpoint().equals(n.endpoint());
                        entries.add(n);
                    }
                }

                owner = new LockOwner(next.endpoint());
                for (LockInvocationKey key : entries) {
                    owner.acquire(key.invocationUid());
                }

                return entries;
            } else {
                owner = null;
            }
        }

        return Collections.emptyList();
    }

    List<Long> invalidateWaitEntries(long sessionId) {
        List<Long> commitIndices = new ArrayList<Long>();
        Iterator<LockInvocationKey> iter = waiters.iterator();
        while (iter.hasNext()) {
            LockInvocationKey entry = iter.next();
            if (sessionId == entry.endpoint().sessionId()) {
                commitIndices.add(entry.commitIndex());
                iter.remove();
            }
        }

        return commitIndices;
    }

    boolean invalidateWaitEntry(LockInvocationKey key) {
        Iterator<LockInvocationKey> iter = waiters.iterator();
        while (iter.hasNext()) {
            LockInvocationKey waiter = iter.next();
            if (waiter.equals(key)) {
                iter.remove();
                return true;
            }
        }

        return false;
    }

    LockOwner owner() {
        return owner;
    }

    RaftLockSnapshot toSnapshot() {
        return new RaftLockSnapshot(groupId, name, owner, waiters);
    }

    static class LockOwner implements IdentifiedDataSerializable {
        private LockEndpoint endpoint;
        private Set<UUID> acquireUids = new HashSet<UUID>();
        private Set<UUID> releaseUids = new HashSet<UUID>();
        private int releaseCount;

        public LockOwner() {
        }

        LockOwner(LockEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        LockEndpoint endpoint() {
            return endpoint;
        }

        private void acquire(UUID acquireUid) {
            acquireUids.add(acquireUid);
        }

        private void release(UUID releaseUid, int releaseCount) {
            if (releaseUids.add(releaseUid)) {
                this.releaseCount = Math.min(acquireUids.size(), this.releaseCount + releaseCount);
            }
        }

        int lockCount() {
            return acquireUids.size() - releaseCount;
        }

        @Override
        public int getFactoryId() {
            return RaftLockDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return RaftLockDataSerializerHook.LOCK_OWNER;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeObject(endpoint);
            out.writeInt(acquireUids.size());
            for (UUID uuid : acquireUids) {
                out.writeLong(uuid.getLeastSignificantBits());
                out.writeLong(uuid.getMostSignificantBits());
            }
            out.writeInt(releaseUids.size());
            for (UUID uuid : releaseUids) {
                out.writeLong(uuid.getLeastSignificantBits());
                out.writeLong(uuid.getMostSignificantBits());
            }
            out.writeInt(releaseCount);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            endpoint = in.readObject();
            int acquireUidCount = in.readInt();
            for (int i = 0; i < acquireUidCount; i++) {
                long least = in.readLong();
                long most = in.readLong();
                acquireUids.add(new UUID(most, least));
            }
            int releaseUidCount = in.readInt();
            for (int i = 0; i < releaseUidCount; i++) {
                long least = in.readLong();
                long most = in.readLong();
                releaseUids.add(new UUID(most, least));
            }
            releaseCount = in.readInt();
        }
    }

}
