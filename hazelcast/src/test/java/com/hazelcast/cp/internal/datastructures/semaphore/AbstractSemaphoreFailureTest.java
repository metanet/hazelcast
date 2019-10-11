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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public abstract class AbstractSemaphoreFailureTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected ProxySessionManagerService sessionManagerService;
    protected ISemaphore semaphore;
    protected String objectName = "semaphore";

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        semaphore = proxyInstance.getCPSubsystem().getSemaphore(getProxyName());
        sessionManagerService = getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getProxyName();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
    }

    abstract boolean isJDKCompatible();

    private RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((SessionAwareProxy) semaphore).getGroupId();
    }

    abstract long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId);

    long getThreadId(RaftGroupId groupId) {
        return sessionManagerService.getOrCreateUniqueThreadId(groupId);
    }

    @Test(timeout = 300_000)
    public void testRetriedAcquireDoesNotCancelWaitingAcquireRequestWhenNoPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, -1));

        assertTrueAllTheTime(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(1, registry.getWaitTimeouts().size());
        }, 10);
    }

    @Test(timeout = 300_000)
    public void testNewAcquireCancelsWaitingAcquireCallWhenNoPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, -1));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewAcquireCancelsWaitingAcquireCallWhenInsufficientPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, -1));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testTryAcquireWithTimeoutCancelsWaitingAcquireCallWhenNoPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 100));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithTimeoutCancelsWaitingAcquireCallWhenInsufficientPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 100));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithoutTimeoutCancelsWaitingAcquireCallWhenNoPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 0));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithoutTimeoutCancelsWaitingAcquireCallWhenInsufficientPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 0));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testReleaseCancelsWaitingAcquireCallWhenNoPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        try {
            semaphore.release();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testReleaseCancelsWaitingAcquireCallWhenInsufficientPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        try {
            semaphore.release();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testDrainCancelsWaitingAcquireCallWhenInsufficientPermitsAvailable() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        semaphore.drainPermits();

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testRetriedAcquireReceivesPermitsOnlyOnce() throws InterruptedException, ExecutionException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        spawn(() -> {
            try {
                semaphore.tryAcquire(20, 5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(2, registry.getWaitTimeouts().size());
        });

        InternalCompletableFuture<Object> f2 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            Semaphore semaphore = registry.getResourceOrNull(objectName);
            assertEquals(2, semaphore.getInternalWaitKeysMap().size());
        });

        spawn(() -> semaphore.increasePermits(3)).get();

        f2.joinInternal();

        assertEquals(2, semaphore.availablePermits());
    }

    @Test(timeout = 300_000)
    public void testExpiredAndRetriedTryAcquireCallReceivesFailureResponse() throws InterruptedException, ExecutionException {
        assumeFalse(isJDKCompatible());

        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Boolean> f1 = invocationManager.invoke(groupId,
                new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, SECONDS.toMillis(5)));

        assertFalse(f1.joinInternal());

        spawn(() -> semaphore.release()).get();

        InternalCompletableFuture<Boolean> f2 = invocationManager.invoke(groupId,
                new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, SECONDS.toMillis(5)));

        assertFalse(f2.joinInternal());
    }

    @Test(timeout = 300_000)
    public void testRetriedDrainCallReceivesPermitsOnlyOnce() throws InterruptedException, ExecutionException {
        assumeFalse(isJDKCompatible());

        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Integer> f1 = invocationManager
                .invoke(groupId, new DrainPermitsOp(objectName, sessionId, threadId, invUid));

        assertEquals(0, (int) f1.joinInternal());

        spawn(() -> semaphore.release()).get();

        InternalCompletableFuture<Integer> f2 = invocationManager
                .invoke(groupId, new DrainPermitsOp(objectName, sessionId, threadId, invUid));

        assertEquals(0, (int) f2.joinInternal());
    }

    @Test(timeout = 300_000)
    public void testAcquireOnMultipleProxies() {
        HazelcastInstance otherInstance = instances[0] == proxyInstance ? instances[1] : instances[0];
        ISemaphore semaphore2 = otherInstance.getCPSubsystem().getSemaphore(semaphore.getName());

        semaphore.init(1);
        semaphore.tryAcquire(1);

        assertFalse(semaphore2.tryAcquire());
    }

    @Test(timeout = 300_000)
    public void testStaleRetryOfWaitingAcquireCallFails() {
        assumeTrue(proxyInstance.getConfig().getCPSubsystemConfig().getCPMemberCount() > 0);
        assumeFalse(isJDKCompatible());

        CountDownLatch acquireLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);

        semaphore.init(1);

        spawn(() -> {
            try {
                semaphore.acquire();
                acquireLatch.countDown();
                releaseLatch.await(300, SECONDS);
                semaphore.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertOpenEventually(acquireLatch);

        // there is a session id now

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(groupId), invUid1, 1, MINUTES.toMillis(5)));

        NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
        SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);

        long[] firstCallId = new long[1];

        assertTrueEventually(() -> {
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertNotNull(registry.getResourceOrNull(objectName));
            assertEquals(1, registry.getWaitTimeouts().size());
            RaftService raftService = getNodeEngineImpl(primaryInstance).getService(RaftService.SERVICE_NAME);
            CountDownLatch latch = new CountDownLatch(1);
            int partitionId = raftService.getCPGroupPartitionId(groupId);
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    Semaphore semaphore = registry.getResourceOrNull(objectName);
                    Map<Object, SemaphoreInvocationKey> waitKeys = semaphore.getInternalWaitKeysMap();
                    assertEquals(1, waitKeys.size());
                    firstCallId[0] = waitKeys.values().iterator().next().callId();
                    latch.countDown();
                }
            });

            latch.await(60, SECONDS);
        });

        RaftNodeImpl leader = getLeaderNode(instances, groupId);
        AcquirePermitsOp op = new AcquirePermitsOp(objectName, sessionId, getThreadId(groupId), invUid2, 1, MINUTES.toMillis(5));
        op.setCaller(getAddress(proxyInstance), firstCallId[0] - 1);

        InternalCompletableFuture f2 = leader.replicate(op);
        try {
            f2.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testStaleRetryFails() {
        assumeTrue(proxyInstance.getConfig().getCPSubsystemConfig().getCPMemberCount() > 0);
        assumeFalse(isJDKCompatible());

        CountDownLatch acquireLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);

        semaphore.init(1);

        spawn(() -> {
            try {
                semaphore.acquire();
                acquireLatch.countDown();
                releaseLatch.await(300, SECONDS);
                semaphore.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertOpenEventually(acquireLatch);

        // there is a session id now

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Boolean> f1 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, getThreadId(groupId), invUid1, 1, MINUTES.toMillis(5)));

        NodeEngineImpl nodeEngine = getNodeEngineImpl(primaryInstance);
        SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);

        long[] firstCallId = new long[1];

        assertTrueEventually(() -> {
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertNotNull(registry.getResourceOrNull(objectName));
            assertEquals(1, registry.getWaitTimeouts().size());
            RaftService raftService = getNodeEngineImpl(primaryInstance).getService(RaftService.SERVICE_NAME);
            CountDownLatch latch = new CountDownLatch(1);
            int partitionId = raftService.getCPGroupPartitionId(groupId);
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    Semaphore semaphore = registry.getResourceOrNull(objectName);
                    Map<Object, SemaphoreInvocationKey> waitKeys = semaphore.getInternalWaitKeysMap();
                    assertEquals(1, waitKeys.size());
                    firstCallId[0] = waitKeys.values().iterator().next().callId();
                    latch.countDown();
                }
            });

            latch.await(60, SECONDS);
        });

        invocationManager
                .invoke(groupId, new ExpireWaitKeysOp(SemaphoreService.SERVICE_NAME, singleton(BiTuple.of(objectName, invUid1))))
                .joinInternal();

        boolean acquired = f1.joinInternal();
        assertFalse(acquired);

        RaftNodeImpl leader = getLeaderNode(instances, groupId);
        AcquirePermitsOp op = new AcquirePermitsOp(objectName, sessionId, getThreadId(groupId), invUid2, 1, MINUTES.toMillis(5));
        op.setCaller(getAddress(proxyInstance), firstCallId[0] - 1);

        InternalCompletableFuture f2 = leader.replicate(op);
        try {
            f2.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    private AbstractProxySessionManager getSessionManager() {
        return getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

}
