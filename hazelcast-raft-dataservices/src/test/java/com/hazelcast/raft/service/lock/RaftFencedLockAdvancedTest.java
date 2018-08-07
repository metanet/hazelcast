package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.operation.snapshot.RestoreSnapshotOp;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.service.blocking.ResourceRegistry;
import com.hazelcast.raft.service.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.service.lock.RaftFencedLockBasicTest.lockByOtherThread;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private FencedLock lock;
    private String name = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        lock = createLock(name);
        assertNotNull(lock);
    }

    private FencedLock createLock(String name) {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        NodeEngineImpl nodeEngine = getNodeEngineImpl(lockInstance);
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftLockService lockService = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        try {
            RaftGroupId groupId = lockService.createRaftGroup(name).get();
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftFencedLockProxy(raftService.getInvocationManager(), sessionManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(name, groupSize));
        raftConfig.setSessionTimeToLiveSeconds(10);
        raftConfig.setSessionHeartbeatIntervalMillis(SECONDS.toMillis(1));

        RaftLockConfig lockConfig = new RaftLockConfig(name, name);
        config.addRaftLockConfig(lockConfig);
        return config;
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testSuccessfulTryLockClearsWaitTimeouts() {
        lock.lock();

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, MINUTES);
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testFailedTryLockClearsWaitTimeouts() throws InterruptedException {
        lockByOtherThread(lock);

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        long fence = lock.tryLock(1, TimeUnit.SECONDS);

        assertEquals(RaftLockService.INVALID_FENCE, fence);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        lockByOtherThread(lock);

        RaftGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        lock.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        final long fence = this.lock.lock();
        assertTrue(fence > 0);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        final RaftGroupId groupId = this.lock.getGroupId();

        spawn(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
                    lock.isLocked();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastInstance leader = getLeaderInstance(instances, groupId);
                RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
                ResourceRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                    assertTrue(snapshotEntry.index() > 0);
                    List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                    for (RestoreSnapshotOp op : ops) {
                        if (op.getServiceName().equals(RaftLockService.SERVICE_NAME)) {
                            ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                            assertFalse(registry.getWaitTimeouts().isEmpty());
                            return;
                        }
                    }
                    fail();
                }
            }
        });

        HazelcastInstance instanceToShutdown = (instances[0] == lockInstance) ? instances[1] : instances[0];
        instanceToShutdown.shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        getRaftService(newInstance).triggerRaftMemberPromotion().get();
        getRaftService(newInstance).triggerRebalanceRaftGroups().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl raftNode = getRaftNode(newInstance, groupId);
                assertNotNull(raftNode);
                assertTrue(getSnapshotEntry(raftNode).index() > 0);

                RaftLockService service = getNodeEngineImpl(newInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertTrue(registry.getLockCount(name, null) > 0);
                assertEquals(fence, registry.getLockFence(name, null));
            }
        });
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() {
        lock.lock();

        final RaftGroupId groupId = lock.getGroupId();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(groupId).isEmpty());
                }
            }
        });

        lock.forceUnlock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertTrue(service.getSessions(groupId).isEmpty());
                }

                SessionManagerService service = getNodeEngineImpl(lockInstance).getService(SessionManagerService.SERVICE_NAME);
                assertEquals(NO_SESSION_ID, service.getSession(groupId));
            }
        });
    }

    @Test
    public void testActiveSessionIsNotClosed() {
        lock.lock();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(lock.getGroupId()).isEmpty());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(lock.getGroupId()).isEmpty());
                }
            }
        }, 20);
    }

}
