package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.impl.InternalMigrationListenerTest.InternalMigrationListenerImpl;
import com.hazelcast.partition.impl.InternalMigrationListenerTest.MigrationProgressNotification;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.partition.impl.InternalMigrationListenerTest.MigrationProgressEvent.COMMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitTest
        extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    @Test
    public void shouldCommitMigrationWhenMasterIsMigrationSource() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        final Config config2 = createConfig();
        config2.setLiteMember(true);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz1Partition = getOwnedPartition(hz1);
        final InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz1Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz1Partition, hz3Partition);
        assertFalse(hz1Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsDestination() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        hz2.getLifecycleService().terminate();

        waitAllForSafeState(hz1);

        final InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz1), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz1), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsNotMigrationEndpoint() {
        final Config config1 = createConfig();
        config1.setLiteMember(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz2Partition = getOwnedPartition(hz2);
        final InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz2Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz2Partition, hz3Partition);
        assertFalse(hz2Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldRollbackMigrationWhenMasterCrashesBeforeCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        config1.addListenerConfig(new ListenerConfig(new TerminateOnMigrationComplete(migrationStartLatch)));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final CollectMigrationTaskOnRollback listener2 = new CollectMigrationTaskOnRollback();
        final Config config2 = createConfig();
        config2.addListenerConfig(new ListenerConfig(listener2));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final CollectMigrationTaskOnRollback listener3 = new CollectMigrationTaskOnRollback();
        final Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(listener3));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz2);
        assertClusterSizeEventually(3, hz3);

        migrationStartLatch.countDown();

        assertClusterSizeEventually(2, hz2);
        assertClusterSizeEventually(2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener2.rollback);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener3.rollback);
            }
        });

        waitAllForSafeState(hz2, hz3);

        factory.terminateAll();
    }

    // this test fails when migration starts and master crashes before 3rd node completes the start process.
    // I couldn't figure out the problem yet
    @Test
    @Ignore
    public void shouldRollbackMigrationWhenMasterCrashesBeforeCommit2() {
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        config1.addListenerConfig(new ListenerConfig(new TerminateOnMigrationComplete(null)));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final CollectMigrationTaskOnRollback listener2 = new CollectMigrationTaskOnRollback();
        final Config config2 = createConfig();
        config2.addListenerConfig(new ListenerConfig(listener2));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final CollectMigrationTaskOnRollback listener3 = new CollectMigrationTaskOnRollback();
        final Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(listener3));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSizeEventually(2, hz2);
        assertClusterSizeEventually(2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener2.rollback);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener3.rollback);
            }
        });

        waitAllForSafeState(hz2, hz3);

        factory.terminateAll();
    }


    @Test
    public void shouldRollbackMigrationWhenDestinationCrashesBeforeCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final CrashOtherMemberOnMigrationComplete masterListener = new CrashOtherMemberOnMigrationComplete(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        masterListener.other = hz3;
        migrationStartLatch.countDown();

        sleepAtLeastSeconds(10);

        waitAllForSafeState(hz1, hz2);

        final InternalPartition partition0 = getPartitionService(hz2).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz2).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback);
    }

    @Test
    public void shouldCommitMigrationWhenSourceFailsDuringCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final CrashOtherMemberOnMigrationComplete masterListener = new CrashOtherMemberOnMigrationComplete(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();
        final InternalMigrationListenerImpl targetListener = new InternalMigrationListenerImpl();
        config3.addListenerConfig(new ListenerConfig(targetListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        masterListener.other = hz2;
        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz3);

        final InternalPartition partition0 = getPartitionService(hz3).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz3).getPartition(1);

        assertEquals(getAddress(hz3), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz3), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertFalse(masterListener.rollback);
        final List<MigrationProgressNotification> notifications = targetListener.getNotifications();
        assertFalse(notifications.isEmpty());
        assertEquals(COMMIT, notifications.get(notifications.size() - 1).event);
    }

    @Test
    public void shouldRollbackMigrationWhenDestinationCrashesDuringCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final DelayMigrationStartOnMaster masterListener = new DelayMigrationStartOnMaster(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final CountDownLatch terminationLatch = new CountDownLatch(1);
        final TerminateOnMigrationCommit memberListener = new TerminateOnMigrationCommit(terminationLatch);
        final Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(memberListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        warmUpPartitions(hz1, hz2, hz3);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2);

        final InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback.get());

        terminationLatch.countDown();
    }

    private Config createConfig() {
        final Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS, "0");
        config.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config.setProperty("hazelcast.logging.type", "log4j");
        return config;
    }

    private InternalPartition getOwnedPartition(final HazelcastInstance instance) {
        final InternalPartitionService partitionService = getPartitionService(instance);
        final Address address = getAddress(instance);
        if (address.equals(partitionService.getPartitionOwner(0))) {
            return partitionService.getPartition(0);
        } else if (address.equals(partitionService.getPartitionOwner(1))) {
            return partitionService.getPartition(1);
        }
        return null;
    }

    private static void resetInternalPartitionListener(HazelcastInstance instance) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        partitionService.resetInternalMigrationListener();
    }

    public static class DelayMigrationStartOnMaster
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private final AtomicBoolean rollback = new AtomicBoolean();

        private HazelcastInstance instance;

        public DelayMigrationStartOnMaster(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            rollback.compareAndSet(false, true);
            resetInternalPartitionListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class CrashOtherMemberOnMigrationComplete
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile boolean rollback;

        private volatile HazelcastInstance instance;

        private volatile HazelcastInstance other;

        public CrashOtherMemberOnMigrationComplete(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            if (!success) {
                System.err.println("ERR: migration is not successful");
            }

            final int memberCount = instance.getCluster().getMembers().size();
            other.getLifecycleService().terminate();
            assertClusterSizeEventually(memberCount - 1, instance);
        }

        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            rollback = true;

            resetInternalPartitionListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class TerminateOnMigrationCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch latch;

        private volatile HazelcastInstance instance;

        public TerminateOnMigrationCommit(CountDownLatch latch) {
            this.latch = latch;
        }

        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            spawn(new Runnable() {
                @Override
                public void run() {
                    instance.getLifecycleService().terminate();
                }
            });

            assertOpenEventually(latch);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }


    private static class TerminateOnMigrationComplete
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile HazelcastInstance instance;

        public TerminateOnMigrationComplete(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (migrationStartLatch != null) {
                assertOpenEventually(migrationStartLatch);
            }
        }

        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            if (!success) {
                System.err.println("ERR: migration is not successful " + migrationInfo + " participant: " + participant);
            }

            resetInternalPartitionListener(instance);
            instance.getLifecycleService().terminate();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class CollectMigrationTaskOnRollback
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private HazelcastInstance instance;

        private volatile boolean rollback = false;

        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (!migrationInfoRef.compareAndSet(null, migrationInfo)) {
                System.err.println("COLLECT START FAILED! curr: " + migrationInfoRef.get() + " new: " + migrationInfo);
            }
        }

        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            MigrationInfo collected = migrationInfoRef.get();
            rollback = migrationInfo.equals(collected);
            if (rollback) {
                migrationInfoRef.set(null);
                final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(
                        instance);
                partitionService.resetInternalMigrationListener();
            } else {
                System.err.println(
                        "collect rollback failed! collected migration: " + collected + " rollback migration: " + migrationInfo
                                + " participant: " + participant);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

}
