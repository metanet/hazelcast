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

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class DrainPermitsOp extends AbstractSemaphoreOp {

    public DrainPermitsOp() {
    }

    public DrainPermitsOp(String name, long sessionId, UUID invocationUid) {
        super(name, sessionId, invocationUid);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        return service.drainPermits(groupId, name, sessionId, invocationUid);
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.DRAIN_PERMITS_OP;
    }
}
