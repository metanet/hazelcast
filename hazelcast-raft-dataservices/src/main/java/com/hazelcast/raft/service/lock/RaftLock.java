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
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    private LockInvocationKey owner;
    private int lockCount;
    private UUID releaseRefUid;

    public RaftLock() {
    }

    RaftLock(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    public Collection<Long> getOwnerSessionIds() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.<Long>emptyList();
    }

    long acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (owner != null && owner.invocationUid().equals(invocationUid)) {
            return owner.commitIndex();
        }

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            lockCount++;
            return owner.commitIndex();
        }

        if (wait) {
            waitKeys.add(key);
        }

        return INVALID_FENCE;
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    private Collection<LockInvocationKey> release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            releaseRefUid = invocationUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return Collections.emptyList();
            }

            LockInvocationKey nextOwner = waitKeys.poll();
            if (nextOwner != null) {
                List<LockInvocationKey> entries = new ArrayList<LockInvocationKey>();
                entries.add(nextOwner);

                Iterator<LockInvocationKey> iter = waitKeys.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey n = iter.next();
                    if (nextOwner.invocationUid().equals(n.invocationUid())) {
                        iter.remove();
                        assert nextOwner.endpoint().equals(n.endpoint());
                        entries.add(n);
                    }
                }

                owner = nextOwner;
                lockCount = 1;
                return entries;
            } else {
                owner = null;
            }

            return Collections.emptyList();
        }

        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    Collection<LockInvocationKey> forceRelease(long expectedFence, UUID invocationUid) {
        // if forceRelease() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner == null) {
            throw new IllegalMonitorStateException();
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), lockCount, invocationUid);
        }

        throw new IllegalMonitorStateException();
    }

    int lockCount() {
        return lockCount;
    }

    LockInvocationKey owner() {
        return owner;
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
        if (owner != null && sessionId == owner.endpoint().sessionId()) {
            Collection<LockInvocationKey> w = release(owner.endpoint(), Integer.MAX_VALUE, UuidUtil.newUnsecureUUID());
            if (w.isEmpty()) {
                return;
            }
            Object newOwnerCommitIndex = w.iterator().next().commitIndex();
            for (LockInvocationKey waitEntry : w) {
                result.put(waitEntry.commitIndex(), newOwnerCommitIndex);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        boolean hasRefUid = (releaseRefUid != null);
        out.writeBoolean(hasRefUid);
        if (hasRefUid) {
            out.writeLong(releaseRefUid.getLeastSignificantBits());
            out.writeLong(releaseRefUid.getMostSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        boolean hasRefUid = in.readBoolean();
        if (hasRefUid) {
            long least = in.readLong();
            long most = in.readLong();
            releaseRefUid = new UUID(most, least);
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + "groupId=" + groupId + ", name='" + name + '\'' + ", owner=" + owner + ", lockCount=" + lockCount
                + ", releaseRefUid=" + releaseRefUid + ", waitKeys=" + waitKeys + '}';
    }
}
