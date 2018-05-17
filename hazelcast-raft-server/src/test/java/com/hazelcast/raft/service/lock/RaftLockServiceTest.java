package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftAtomicLongConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.config.raft.RaftServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.session.SessionResponse;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockServiceTest extends HazelcastRaftTestSupport {

    private static final String RAFT_GROUP_NAME = "locks";
    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private RaftInvocationManager invocationManager;
    private RaftGroupId groupId;
    private int partitionId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        Address[] raftAddresses = createAddresses(raftGroupSize);
        instances = newInstances(raftAddresses, raftGroupSize, 0);
        invocationManager = getRaftInvocationService(instances[0]);
        groupId = invocationManager.createRaftGroup(RAFT_GROUP_NAME, raftGroupSize).get();
        partitionId = getNodeEngineImpl(instances[0]).getPartitionService().getPartitionId(groupId);
    }

    @Test
    public void testLockInvocationIdempotency() throws ExecutionException, InterruptedException {
        SessionResponse session = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        long threadId = ThreadUtil.getThreadId();
        UUID lockInvocationUid = UuidUtil.newUnsecureUUID();
        invocationManager.invoke(groupId, new LockOp(RAFT_GROUP_NAME, session.getSessionId(), threadId, lockInvocationUid)).get();

        // I have the lock
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                InternalOperationService operationService = getNodeEngineImpl(instances[0]).getOperationService();
                final boolean[] locked = new boolean[1];
                operationService.execute(new PartitionSpecificRunnable() {
                    @Override
                    public int getPartitionId() {
                        return partitionId;
                    }

                    @Override
                    public void run() {
                        RaftLockService lockService = getNodeEngineImpl(instances[0]).getService(RaftLockService.SERVICE_NAME);
                        LockRegistry registry = lockService.getLockRegistryOrNull(groupId);
                        if (registry != null) {
                            RaftLock lock = registry.getRaftLockOrNull(RAFT_GROUP_NAME);
                            locked[0] = (lock != null && lock.owner() != null);
                        }
                    }
                });
                assertTrue(locked[0]);
            }
        });

        invocationManager.invoke(groupId, new LockOp(RAFT_GROUP_NAME, session.getSessionId(), threadId, lockInvocationUid)).get();

        // lock count is still 1
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                InternalOperationService operationService = getNodeEngineImpl(instances[0]).getOperationService();
                final int[] lockCount = new int[1];
                operationService.execute(new PartitionSpecificRunnable() {
                    @Override
                    public int getPartitionId() {
                        return partitionId;
                    }

                    @Override
                    public void run() {
                        RaftLockService lockService = getNodeEngineImpl(instances[0]).getService(RaftLockService.SERVICE_NAME);
                        LockRegistry registry = lockService.getLockRegistryOrNull(groupId);
                        RaftLock lock = registry.getRaftLockOrNull(RAFT_GROUP_NAME);
                        lockCount[0] = lock.owner().lockCount();
                    }
                });
                assertEquals(1, lockCount[0]);
            }
        });
    }

    @Test
    public void testSnapshotRestore() throws ExecutionException, InterruptedException {
        final HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final HazelcastInstance follower = getRandomFollowerInstance(instances, groupId);

        // the follower falls behind the leader. It neither append entries nor installs snapshots.
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        final ILock lock = ((RaftLockService) getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME)).createNew(RAFT_GROUP_NAME);
        lockByOtherThread(lock);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, TimeUnit.MINUTES);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getLockRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getTryLockTimeouts().isEmpty());
            }
        });

        IAtomicLong atomicLong = ((RaftAtomicLongService) getNodeEngineImpl(instances[0]).getService(RaftAtomicLongService.SERVICE_NAME)).createNew(RAFT_GROUP_NAME);
        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            atomicLong.incrementAndGet();
        }

        final RaftNodeImpl leaderRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(leader).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);
        final RaftNodeImpl followerRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(follower).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);

        // the leader takes a snapshot
        final long[] leaderSnapshotIndex = new long[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long idx = getSnapshotEntry(leaderRaftNode).index();
                assertTrue(idx > 0);
                leaderSnapshotIndex[0] = idx;
            }
        });

        // the follower doesn't have it since its raft log is still empty
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getSnapshotEntry(followerRaftNode).index());
            }
        }, 10);

        resetPacketFiltersFrom(leader);

        // the follower installs the snapshot after it hears from the leader
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(leaderSnapshotIndex[0], getSnapshotEntry(followerRaftNode).index());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(follower).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getLockRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getTryLockTimeouts().isEmpty());
            }
        });
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        Config config = super.createConfig(raftAddresses, metadataGroupSize);
        RaftServiceConfig raftServiceConfig = config.getRaftServiceConfig();
        raftServiceConfig.getRaftConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftServiceConfig.addGroupConfig(new RaftGroupConfig(RAFT_GROUP_NAME, 3));
        config.addRaftLockConfig(new RaftLockConfig(RAFT_GROUP_NAME, RAFT_GROUP_NAME));
        config.addRaftAtomicLongConfig(new RaftAtomicLongConfig(RAFT_GROUP_NAME, RAFT_GROUP_NAME));

        return config;
    }
}
