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
import com.hazelcast.raft.service.lock.RaftLock.LockOwner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockSnapshot implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private String name;
    private LockOwner owner;
    private List<LockInvocationKey> waiters;

    public RaftLockSnapshot() {
    }

    RaftLockSnapshot(RaftGroupId groupId, String name, LockOwner owner, List<LockInvocationKey> waiters) {
        this.groupId = groupId;
        this.name = name;
        this.owner = owner;
        this.waiters = new ArrayList<LockInvocationKey>(waiters);
    }

    RaftGroupId getGroupId() {
        return groupId;
    }

    String getName() {
        return name;
    }

    LockOwner getOwner() {
        return owner;
    }

    List<LockInvocationKey> getWaiters() {
        return waiters;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeUTF(name);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(waiters.size());
        for (LockInvocationKey key : waiters) {
            out.writeObject(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        name = in.readUTF();
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        int count = in.readInt();
        waiters = new ArrayList<LockInvocationKey>();
        for (int i = 0; i < count; i++)  {
            LockInvocationKey key = in.readObject();
            waiters.add(key);
        }
    }
}
