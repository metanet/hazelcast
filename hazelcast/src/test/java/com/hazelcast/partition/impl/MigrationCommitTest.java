package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitTest
        extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    @Test
    public void shouldLoseDataWhenMigrationDestinationFailsDuringMigrationCommit() {
        final Config config1 = new Config();
        config1.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        final String mapName = randomMapName();
        config1.getMapConfig(mapName).setBackupCount(0);
        config1.setProperty(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS, "10");

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);
        warmUpPartitions(hz1);

        final String partition0Key = generateKeyForPartition(hz1, 0);
        final String partition1Key = generateKeyForPartition(hz1, 1);

        final IMap<Object, Object> map = hz1.getMap(mapName);
        map.put(partition0Key, true);
        map.put(partition1Key, true);

        waitAllForSafeState(hz1);

        final Config config2 = new Config();
        config2.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config2.getMapConfig(mapName).setBackupCount(0);
        config2.addListenerConfig(new ListenerConfig(new InternalMigrationListenerImpl()));
        config2.setProperty(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS, "10");
        factory.newHazelcastInstance(config2);

        assertClusterSizeEventually(1, hz1);
        assertTrue(map.containsKey(partition0Key) ^ map.containsKey(partition1Key));

    }

    private static class InternalMigrationListenerImpl extends InternalMigrationListener implements HazelcastInstanceAware {


        private HazelcastInstance instance;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {

        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {

        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            sleepAtLeastSeconds(10);
            instance.getLifecycleService().terminate();
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {

        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

}
