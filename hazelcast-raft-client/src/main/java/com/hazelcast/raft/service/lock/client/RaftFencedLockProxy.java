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

package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.proxy.AbstractRaftFencedLockProxy;
import com.hazelcast.raft.service.session.SessionManagerProvider;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.UUID;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.FORCE_UNLOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_COUNT_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_FENCE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.UNLOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.BOOLEAN_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.INT_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.LONG_RESPONSE_DECODER;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.encodeRequest;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.invoke;
import static com.hazelcast.raft.service.lock.client.RaftLockProxy.prepareClientMessage;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

/**
 * TODO: Javadoc Pending...
 */
public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    public static FencedLock create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(name);
        msg.updateFrameLength();

        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, msg, name).invoke();

        InternalCompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public RaftGroupId decodeClientMessage(ClientMessage msg) {
                        return RaftGroupIdImpl.readFrom(msg);
                    }
                });

        RaftGroupId groupId = future.join();
        return new RaftFencedLockProxy(instance, groupId, name);
    }

    private final HazelcastClientInstanceImpl client;

    private RaftFencedLockProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        super(SessionManagerProvider.get(getClient(instance)), groupId, name);
        this.client = getClient(instance);
    }

    @Override
    protected InternalCompletableFuture<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        ClientMessage msg = encodeRequest(LOCK_TYPE, groupId, name, sessionId, threadId, invocationUid);
        return invoke(client, name, msg, LONG_RESPONSE_DECODER);
    }

    @Override
    protected InternalCompletableFuture<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid,
                                     long timeoutMillis) {
        ClientMessage msg = encodeRequest(TRY_LOCK_TYPE, groupId, name, sessionId, threadId, invocationUid, timeoutMillis);
        return invoke(client, name, msg, LONG_RESPONSE_DECODER);
    }

    @Override
    protected InternalCompletableFuture<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        ClientMessage msg = encodeRequest(UNLOCK_TYPE, groupId, name, sessionId, threadId, invocationUid);
        return invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected InternalCompletableFuture<Object> doForceUnlock(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        ClientMessage msg = encodeRequest(FORCE_UNLOCK_TYPE, groupId, name, -1, -1, invocationUid, expectedFence);
        return invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected InternalCompletableFuture<Long> doGetLockFence(RaftGroupId groupId, String name, long sessionId, long threadId) {
        ClientMessage msg = encodeRequest(LOCK_FENCE_TYPE, groupId, name, sessionId, threadId);
        return invoke(client, name, msg, LONG_RESPONSE_DECODER);
    }

    @Override
    protected InternalCompletableFuture<Integer> doGetLockCount(RaftGroupId groupId, String name, long sessionId, long threadId) {
        ClientMessage msg = encodeRequest(LOCK_COUNT_TYPE, groupId, name, sessionId, threadId);
        return invoke(client, name, msg, INT_RESPONSE_DECODER);
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER).join();
    }

}
