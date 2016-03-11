package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalMigrationListenerTest
        extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    @Test
    public void shouldInvokeInternalMigrationListenerOnSuccessfulMigration() {
        final Config config1 = new Config();
        config1.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);
        warmUpPartitions(hz1);

        final InternalMigrationListenerImpl listener = new InternalMigrationListenerImpl();
        final Config config2 = new Config();
        config2.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config2.addListenerConfig(new ListenerConfig(listener));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        waitAllForSafeState(hz1, hz2);

        final List<Integer> hz2PartitionIds = getNodeEngineImpl(hz2).getPartitionService().getMemberPartitions(getAddress(hz2));
        assertEquals(1, hz2PartitionIds.size());
        final int hz2PartitionId = hz2PartitionIds.get(0);

        final List<MigrationProgressNotification> notifications = listener.getNotifications();
        assertEquals(3, notifications.size());
        assertEquals(MigrationProgressEvent.START, notifications.get(0).event);
        assertEquals(hz2PartitionId, notifications.get(0).migrationInfo.getPartitionId());
        assertEquals(MigrationProgressEvent.COMPLETE, notifications.get(1).event);
        assertEquals(hz2PartitionId, notifications.get(1).migrationInfo.getPartitionId());
        assertTrue(notifications.get(1).success);
        assertEquals(MigrationProgressEvent.COMMIT, notifications.get(2).event);
        assertEquals(hz2PartitionId, notifications.get(2).migrationInfo.getPartitionId());
    }

    private enum MigrationProgressEvent {
        START,
        COMPLETE,
        COMMIT,
        ROLLBACK
    }

    private static class MigrationProgressNotification {

        final MigrationProgressEvent event;

        final MigrationParticipant participant;

        final MigrationInfo migrationInfo;

        final boolean success;

        public MigrationProgressNotification(MigrationProgressEvent event, MigrationParticipant participant,
                                             MigrationInfo migrationInfo) {
            this(event, participant, migrationInfo, true);
        }

        public MigrationProgressNotification(MigrationProgressEvent event, MigrationParticipant participant,
                                             MigrationInfo migrationInfo, boolean success) {
            this.event = event;
            this.participant = participant;
            this.migrationInfo = migrationInfo;
            this.success = success;
        }

    }

    private static class InternalMigrationListenerImpl
            implements InternalMigrationListener {

        private final List<MigrationProgressNotification> notifications = new ArrayList<MigrationProgressNotification>();

        @Override
        public synchronized void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.START, participant, migrationInfo));
        }

        @Override
        public synchronized void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo,
                                                     boolean success) {
            notifications
                    .add(new MigrationProgressNotification(MigrationProgressEvent.COMPLETE, participant, migrationInfo, success));
        }

        @Override
        public synchronized void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.COMMIT, participant, migrationInfo));
        }

        @Override
        public synchronized void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.ROLLBACK, participant, migrationInfo));
        }

        public synchronized List<MigrationProgressNotification> getNotifications() {
            return new ArrayList<MigrationProgressNotification>(notifications);
        }

    }

}
