package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public void shouldRollbackMigrationWhenDestinationCrashesBeforeCommit() {
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
        final TerminateMigrationDestinationOnCommit memberListener = new TerminateMigrationDestinationOnCommit(
                terminationLatch);
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
        config.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
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

    public static class DelayMigrationStartOnMaster
            extends InternalMigrationListener {

        private final CountDownLatch migrationStartLatch;

        private final AtomicBoolean rollback = new AtomicBoolean();

        public DelayMigrationStartOnMaster(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            rollback.compareAndSet(false, true);
        }

    }

    private static class TerminateMigrationDestinationOnCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch latch;

        private volatile HazelcastInstance instance;

        public TerminateMigrationDestinationOnCommit(CountDownLatch latch) {
            this.latch = latch;
        }

        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            System.out.println(">>>> " + migrationInfo);

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

}
