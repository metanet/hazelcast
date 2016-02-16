package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;


    @Test
    public void shouldCommitMigrationWhenMasterIsMigrationSource() {
        final Config config1 = createConfig();

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final Config config2 = createConfig();
        config2.setLiteMember(true);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();

        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz1Partition = getPartition(hz1);
        final InternalPartition hz3Partition = getPartition(hz3);
        assertNotNull(hz1Partition);
        assertNotNull(hz3Partition);
        assertFalse(hz1Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsNotMigrationEndpoint() {
        final Config config1 = createConfig();
        config1.setLiteMember(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final Config config2 = createConfig();

        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();

        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz1Partition = getPartition(hz1);
        final InternalPartition hz3Partition = getPartition(hz3);
        assertNotNull(hz1Partition);
        assertNotNull(hz3Partition);
        assertFalse(hz1Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    private Config createConfig() {
        final Config config1 = new Config();
        config1.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        return config1;
    }

    private InternalPartition getPartition(final HazelcastInstance instance) {
        final InternalPartitionService partitionService = getPartitionService(instance);
        final int partitionId = getAddress(instance).equals(partitionService.getPartitionOwner(0)) ? 0 : 1;
        return partitionService.getPartition(partitionId);
    }


}
