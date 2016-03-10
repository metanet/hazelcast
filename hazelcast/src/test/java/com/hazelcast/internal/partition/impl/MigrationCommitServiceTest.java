package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.partition.impl.MigrationCommitTest.resetInternalMigrationListener;
import static com.hazelcast.internal.partition.impl.MigrationManager.MigrateTaskReason.REPARTITIONING;
import static com.hazelcast.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.partition.MigrationEndpoint.SOURCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitServiceTest
        extends HazelcastTestSupport {

    private static final int NODE_COUNT = 10;

    private static final int PARTITION_COUNT = 10;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(createConfig(), NODE_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);
        for (HazelcastInstance instance : instances) {
            final MigrationEventCollectingService service = getNodeEngineImpl(instance)
                    .getService(MigrationEventCollectingService.SERVICE_NAME);

            service.clear();
        }
    }

    @Test
    public void testPartitionOwnerMoveCommit() {
        final MigrationInfo migration = findPartitionOwnerMigration();

        migrate(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
    }

    @Test
    public void testPartitionOwnerMoveRollback() {
        final MigrationInfo migration = findPartitionOwnerMigration();

        final RejectMigration listener = new RejectMigration();
        final HazelcastInstance destination = factory.getInstance(migration.getDestination());
        listener.instance = destination;
        final InternalPartitionServiceImpl destinationPartitionService = (InternalPartitionServiceImpl) getPartitionService(
                destination);
        destinationPartitionService.getMigrationManager().setInternalMigrationListener(listener);

        migrate(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);
    }

    @Test
    public void testPartitionBackupMoveCommit() {
        final MigrationInfo migration = findPartitionBackupMoveMigration();

        migrate(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
    }

    @Test
    public void testPartitionBackupMoveRollback() {
        final MigrationInfo migration = findPartitionBackupMoveMigration();

        final RejectMigration listener = new RejectMigration();
        final HazelcastInstance destination = factory.getInstance(migration.getDestination());
        listener.instance = destination;
        final InternalPartitionServiceImpl destinationPartitionService = (InternalPartitionServiceImpl) getPartitionService(
                destination);
        destinationPartitionService.getMigrationManager().setInternalMigrationListener(listener);

        migrate(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);
    }

    private void assertMigrationSourceCommit(final MigrationInfo migration) {
        final MigrationEventCollectingService service = getService(migration.getSource());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent sourceCommitEvent = service.getCommitEvents().get(0);

        assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
        assertSourcePartitionMigrationEvent(msg, sourceCommitEvent, migration);
    }

    private String getAssertMessage(MigrationInfo migration, MigrationEventCollectingService service) {
        return migration + " -> BeforeEvents: " + service.getBeforeEvents() + " , CommitEvents: " + service.getCommitEvents()
                + " , RollbackEvents: " + service.getRollbackEvents();
    }

    private void assertMigrationSourceRollback(final MigrationInfo migration) {
        final MigrationEventCollectingService service = getService(migration.getSource());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent sourceRollbackEvent = service.getRollbackEvents().get(0);

        assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
        assertSourcePartitionMigrationEvent(msg, sourceRollbackEvent, migration);
    }

    private void assertMigrationDestinationCommit(final MigrationInfo migration) {
        final MigrationEventCollectingService service = getService(migration.getDestination());

        final String msg = getAssertMessage(migration, service);
        final PartitionMigrationEvent destinationCommitEvent = service.getCommitEvents().get(0);

        assertDestinationPartitionMigrationEvent(msg, destinationCommitEvent, migration);
    }

    private void assertSourcePartitionMigrationEvent(final String msg, final PartitionMigrationEvent event,
                                                     final MigrationInfo migration) {
        assertEquals(msg, SOURCE, event.getMigrationEndpoint());
        assertEquals(msg, migration.getSourceCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getSourceNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private void assertDestinationPartitionMigrationEvent(final String msg, final PartitionMigrationEvent event,
                                                          final MigrationInfo migration) {
        assertEquals(msg, DESTINATION, event.getMigrationEndpoint());
        assertEquals(msg, migration.getDestinationCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getDestinationNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private void assertMigrationDestinationRollback(final MigrationInfo migration) {
        final MigrationEventCollectingService service = getService(migration.getDestination());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent destinationRollbackEvent = service.getRollbackEvents().get(0);

        assertDestinationPartitionMigrationEvent(msg, destinationRollbackEvent, migration);
    }

    private MigrationInfo findPartitionOwnerMigration() {
        int partitionId = -1;
        HazelcastInstance oldOwner = null, newOwner = null;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            final HazelcastInstance instance = instances[1];
            final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
            final InternalPartition partition = partitionService.getPartition(i);
            if (getAddress(instance).equals(partition.getOwnerOrNull())) {
                partitionId = i;
                oldOwner = instance;
                System.out.println("Partition to migrate owner: " + partition);
                break;
            }
        }

        assertNotEquals(-1, partitionId);
        assertNotNull(oldOwner);

        for (int i = 1; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
            final InternalPartition partition = partitionService.getPartition(partitionId);
            if (!partition.isOwnerOrBackup(getAddress(instance))) {
                newOwner = instance;
                break;
            }
        }

        assertNotNull(newOwner);
        System.out.println("New partition owner: " + getAddress(newOwner));

        return new MigrationInfo(partitionId, getAddress(oldOwner), getAddress(newOwner), 0, -1, -1, 0);
    }

    private MigrationInfo findPartitionBackupMoveMigration() {
        int partitionId = -1, replicaIndex = -1;
        HazelcastInstance oldBackup = null, newBackup = null;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            final HazelcastInstance instance = instances[1];
            final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
            final InternalPartition partition = partitionService.getPartition(i);
            final int r = partition.getReplicaIndex(getAddress(instance));
            if (r > 0) {
                partitionId = i;
                replicaIndex = r;
                oldBackup = instance;
                System.out.println("Partition to migrate backup: " + partition + " move backup replica index: " + r);
                break;
            }
        }

        assertNotEquals(-1, partitionId);
        assertNotEquals(-1, replicaIndex);
        assertNotNull(oldBackup);

        for (int i = 1; i < NODE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];
            final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
            final InternalPartition partition = partitionService.getPartition(partitionId);
            if (partition.getReplicaIndex(getAddress(instance)) == -1) {
                newBackup = instance;
                break;
            }
        }

        assertNotNull(newBackup);
        System.out.println("New backup owner: " + getAddress(newBackup));

        return new MigrationInfo(partitionId, getAddress(oldBackup), getAddress(newBackup), replicaIndex, -1, -1, replicaIndex);
    }

    private void migrate(MigrationInfo migration) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        partitionService.getMigrationManager().scheduleMigration(migration, REPARTITIONING);
        waitAllForSafeState(instances);
    }

    private Config createConfig() {
        final Config config = new Config();

        final ServiceConfig serviceConfig = new ServiceConfig().setEnabled(true)
                                                               .setName(MigrationEventCollectingService.SERVICE_NAME)
                                                               .setClassName(MigrationEventCollectingService.class.getName());
        config.getServicesConfig().addServiceConfig(serviceConfig);

        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS, "0");
        config.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config.setProperty("hazelcast.logging.type", "none");
        return config;
    }

    private MigrationEventCollectingService getService(final Address address) {
        return getNodeEngineImpl(factory.getInstance(address)).getService(MigrationEventCollectingService.SERVICE_NAME);
    }

    private static class MigrationEventCollectingService
            implements MigrationAwareService {

        private static final String SERVICE_NAME = MigrationEventCollectingService.class.getSimpleName();

        private final List<PartitionMigrationEvent> beforeEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<PartitionMigrationEvent> commitEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<PartitionMigrationEvent> rollbackEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<Integer> clearPartitionIds = new ArrayList<Integer>();

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
            synchronized (beforeEvents) {
                beforeEvents.add(event);
            }
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            synchronized (commitEvents) {
                commitEvents.add(event);
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            synchronized (rollbackEvents) {
                rollbackEvents.add(event);
            }
        }

        @Override
        public void clearPartitionReplica(int partitionId) {
            synchronized (clearPartitionIds) {
                clearPartitionIds.add(partitionId);
            }
        }

        public List<PartitionMigrationEvent> getBeforeEvents() {
            synchronized (beforeEvents) {
                return new ArrayList<PartitionMigrationEvent>(beforeEvents);
            }
        }

        public List<PartitionMigrationEvent> getCommitEvents() {
            synchronized (commitEvents) {
                return new ArrayList<PartitionMigrationEvent>(commitEvents);
            }
        }

        public List<PartitionMigrationEvent> getRollbackEvents() {
            synchronized (rollbackEvents) {
                return new ArrayList<PartitionMigrationEvent>(rollbackEvents);
            }
        }

        public List<Integer> getClearPartitionIds() {
            synchronized (clearPartitionIds) {
                return new ArrayList<Integer>(clearPartitionIds);
            }
        }

        public void clear() {
            synchronized (beforeEvents) {
                beforeEvents.clear();
            }
            synchronized (commitEvents) {
                commitEvents.clear();
            }
            synchronized (rollbackEvents) {
                rollbackEvents.clear();
            }
            synchronized (clearPartitionIds) {
                clearPartitionIds.clear();
            }
        }
    }

    private static class RejectMigration
            extends InternalMigrationListener {

        private volatile HazelcastInstance instance;

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            resetInternalMigrationListener(instance);
            throw new ExpectedRuntimeException("migration is failed intentionally");
        }

    }

}
