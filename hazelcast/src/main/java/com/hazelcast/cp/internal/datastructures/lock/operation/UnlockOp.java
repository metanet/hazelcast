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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CallerAware;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.lock.Lock;
import com.hazelcast.cp.internal.datastructures.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.LockInvocationKey;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation for {@link FencedLock#unlock()}
 *
 * @see Lock#release(LockInvocationKey)
 */
public class UnlockOp extends AbstractLockOp implements CallerAware, IndeterminateOperationStateAware {

    private Address callerAddress;
    private long callId;

    public UnlockOp() {
    }

    public UnlockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        LockService service = getService();
        getNodeEngine().getLogger(getClass()).warning("WILL PROCESS: " + this);
        LockInvocationKey key = new LockInvocationKey(commitIndex, invocationUid, callerAddress, callId, getLockEndpoint());
        getNodeEngine().getLogger(getClass()).warning("KEY: " + key);
        return service.release(groupId, name, key);
    }

    @Override
    public void setCaller(Address callerAddress, long callId) {
        this.callerAddress = callerAddress;
        this.callId = callId;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.UNLOCK_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(callerAddress);
        out.writeLong(callId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        callerAddress = in.readObject();
        callId = in.readLong();
    }
}
