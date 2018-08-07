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

package com.hazelcast.raft.impl.session.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.impl.session.RaftSessionServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public class CloseInactiveSessionsOp extends RaftOp implements IdentifiedDataSerializable {

    private Collection<Long> sessions;

    public CloseInactiveSessionsOp() {
    }

    public CloseInactiveSessionsOp(Collection<Long> sessions) {
        this.sessions = sessions;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        service.closeInactiveSessions(groupId, sessions);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.CLOSE_INACTIVE_SESSIONS;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(sessions.size());
        for (long sessionId : sessions) {
            out.writeLong(sessionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        sessions = new ArrayList<Long>();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            sessions.add(sessionId);
        }
    }
}
