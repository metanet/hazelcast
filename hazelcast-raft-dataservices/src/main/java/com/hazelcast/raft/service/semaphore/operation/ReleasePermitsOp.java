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

package com.hazelcast.raft.service.semaphore.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class ReleasePermitsOp extends AbstractSemaphoreOp {

    private int permits;

    public ReleasePermitsOp() {
    }

    public ReleasePermitsOp(String name, long sessionId, UUID invocationUid,  int permits) {
        super(name, sessionId, invocationUid);
        this.permits = permits;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        service.releasePermits(groupId, name, sessionId, invocationUid, permits);
        return true;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.RELEASE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        permits = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", permits=").append(permits);
    }
}
