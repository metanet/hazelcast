package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.test.AssertTask;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.partition.impl.MigrationCommitTest.resetInternalMigrationListener;
import static com.hazelcast.internal.partition.impl.MigrationManager.MigrateTaskReason.REPARTITIONING;
import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.spi.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitServiceTest
        extends HazelcastTestSupport {

    private static final int NODE_COUNT = 10;

    private static final int PARTITION_COUNT = 10;

    private static final int BACKUP_COUNT = 6;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance[] instances;

    @Before
    public void setup()
            throws ExecutionException, InterruptedException {
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(createConfig(), NODE_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        final InternalOperationService operationService = getOperationService(instances[0]);
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
            operationService.invokeOnPartition(null, new SamplePutOperation(), partitionId).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    final InternalPartitionService partitionService = getPartitionService(instances[0]);
                    final InternalPartition partition = partitionService.getPartition(partitionId);
                    for (int i = 0; i <= BACKUP_COUNT; i++) {
                        final MigrationEventCollectingService service = getService(partition.getReplicaAddress(i));
                        assertTrue(Boolean.TRUE.equals(service.data.get(partitionId)));
                    }
                }
            }
        });

        for (HazelcastInstance instance : instances) {
            final MigrationEventCollectingService service = getNodeEngineImpl(instance)
                    .getService(MigrationEventCollectingService.SERVICE_NAME);

            service.clearEvents();
        }
    }

    @Test
    public void testPartitionOwnerMoveCommit()
            throws InterruptedException {
        final MigrationInfo migration = findPartitionOwnerMigration();

        migrate(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
    }

    @Test
    public void testPartitionOwnerMoveRollback()
            throws InterruptedException {
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
    public void testPartitionBackupMoveCommit()
            throws InterruptedException {
        final MigrationInfo migration = findPartitionBackupMoveMigration();

        migrate(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
    }

    @Test
    public void testPartitionBackupMoveRollback()
            throws InterruptedException {
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

    @Test
    public void testPartitionOwnerMoveCopyBackCommitWithOldOwnerOfKeepReplicaIndex()
            throws InterruptedException {
        final int partitionId = 5, oldReplicaIndex = 0, newReplicaIndex = 3;
        testMoveCopyBackupWithOldReplicaOwner(partitionId, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionOwnerMoveCopyBackRollbackWithOldOwnerOfKeepReplicaIndex() {

    }

    @Test
    public void testPartitionOwnerMoveCopyBackCommitWithNullOwnerOfKeepReplicaIndex() {

    }

    @Test
    public void testPartitionOwnerMoveCopyBackRollbackWithNullOwnerOfKeepReplicaIndex() {

    }

    @Test
    public void testPartitionBackupMoveCopyBackCommitWithOldOwnerOfKeepReplicaIndex()
            throws InterruptedException {
        final int partitionId = 5, oldReplicaIndex = 2, newReplicaIndex = 4;
        testMoveCopyBackupWithOldReplicaOwner(partitionId, oldReplicaIndex, newReplicaIndex);
    }

    private void testMoveCopyBackupWithOldReplicaOwner(final int partitionId, final int oldReplicaIndex,
                                                       final int newReplicaIndex)
            throws InterruptedException {
        final MigrationInfo migration = createMoveCopyBackMigration(partitionId, oldReplicaIndex, newReplicaIndex);
        migrate(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);

        final MigrationEventCollectingService service = getService(migration.getOldReplicaOwner());
        final List<Integer> clearPartitionIds = service.getClearPartitionIds();

        assertTrue(clearPartitionIds.size() == 1);
        assertEquals(partitionId, (int) clearPartitionIds.get(0));
    }

    @Test
    public void testPartitionBackupMoveCopyBackRollbackWithOldOwnerOfKeepReplicaIndex() {

    }

    @Test
    public void testPartitionBackupMoveCopyBackCommitWithNullOwnerOfKeepReplicaIndex() {

    }

    @Test
    public void testPartitionBackupMoveCopyBackRollbackWithNullOwnerOfKeepReplicaIndex() {

    }

    private MigrationInfo createMoveCopyBackMigration(final int partitionId, final int oldReplicaIndex,
                                                      final int newReplicaIndex) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        final Address source = partition.getReplicaAddress(oldReplicaIndex);
        final Address oldReplicaOwner = partition.getReplicaAddress(newReplicaIndex);

        Address newReplicaOwner = null;
        for (HazelcastInstance instance : instances) {
            final Address address = getAddress(instance);
            if (!partition.isOwnerOrBackup(address)) {
                newReplicaOwner = address;
                break;
            }
        }

        final MigrationInfo migration = new MigrationInfo(partitionId, source, newReplicaOwner, oldReplicaIndex, newReplicaIndex,
                -1, oldReplicaIndex);
        migration.setOldReplicaOwner(oldReplicaOwner);

        return migration;
    }

    private void clearReplicaIndex(final int partitionId, final int replicaIndexToClear) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        final Address oldReplicaOwner = partition.getReplicaAddress(replicaIndexToClear);
        partition.setReplicaAddress(replicaIndexToClear, null);

        final MigrationInfo migration = new MigrationInfo(0, null, null, replicaIndexToClear, -1, -1, -1);
        migration.setOldReplicaOwner(oldReplicaOwner);
        migration.setStatus(MigrationStatus.SUCCESS);
        partitionService.getMigrationManager().addCompletedMigration(migration);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(partitionService.syncPartitionRuntimeState());
            }
        });
    }

    private void assertMigrationSourceCommit(final MigrationInfo migration)
            throws InterruptedException {
        final MigrationEventCollectingService service = getService(migration.getSource());

        String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent sourceCommitEvent = service.getCommitEvents().get(0);

        assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
        assertSourcePartitionMigrationEvent(msg, sourceCommitEvent, migration);

        assertReplicaVersionsAndServiceData(msg, migration.getSource(), migration.getPartitionId(), migration.getSourceNewReplicaIndex());
    }

    private void assertReplicaVersionsAndServiceData(String msg, final Address address, final int partitionId, final int replicaIndex)
            throws InterruptedException {
        final MigrationEventCollectingService service = getService(address);

        final boolean shouldContainData = replicaIndex != -1 && replicaIndex <= BACKUP_COUNT;
        assertEquals(msg, shouldContainData, service.containsDataForPartition(partitionId));

        final long[] replicaVersions = getReplicaVersions(factory.getInstance(address), partitionId);

        msg = msg + " , ReplicaVersions: " + Arrays.toString(replicaVersions);
        if (shouldContainData) {
            if (replicaIndex == 0) {
                assertArrayEquals(msg, replicaVersions, new long[]{1, 1, 1, 1, 1, 1});
            } else {
                for (int i = 1; i < InternalPartition.MAX_BACKUP_COUNT; i++) {
                    if ( i < replicaIndex || i > BACKUP_COUNT) {
                        assertEquals(msg, 0, replicaVersions[i - 1]);
                    } else {
                        assertEquals(msg, 1, replicaVersions[i - 1]);
                    }
                }
            }
        } else {
            assertArrayEquals(msg, replicaVersions, new long[]{0, 0, 0, 0, 0, 0});
        }
    }

    private String getAssertMessage(MigrationInfo migration, MigrationEventCollectingService service) {
        return migration + " -> BeforeEvents: " + service.getBeforeEvents() + " , CommitEvents: " + service.getCommitEvents()
                + " , RollbackEvents: " + service.getRollbackEvents();
    }

    private void assertMigrationSourceRollback(final MigrationInfo migration)
            throws InterruptedException {
        final MigrationEventCollectingService service = getService(migration.getSource());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent sourceRollbackEvent = service.getRollbackEvents().get(0);

        assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
        assertSourcePartitionMigrationEvent(msg, sourceRollbackEvent, migration);

        assertReplicaVersionsAndServiceData(msg, migration.getSource(), migration.getPartitionId(), migration.getSourceCurrentReplicaIndex());
    }

    private void assertMigrationDestinationCommit(final MigrationInfo migration)
            throws InterruptedException {
        final MigrationEventCollectingService service = getService(migration.getDestination());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent destinationCommitEvent = service.getCommitEvents().get(0);

        assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
        assertDestinationPartitionMigrationEvent(msg, destinationCommitEvent, migration);

        assertTrue(msg, service.containsDataForPartition(migration.getPartitionId()));

        assertReplicaVersionsAndServiceData(msg, migration.getDestination(), migration.getPartitionId(), migration.getDestinationNewReplicaIndex());
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

    private void assertMigrationDestinationRollback(final MigrationInfo migration)
            throws InterruptedException {
        final MigrationEventCollectingService service = getService(migration.getDestination());

        final String msg = getAssertMessage(migration, service);

        final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
        final PartitionMigrationEvent destinationRollbackEvent = service.getRollbackEvents().get(0);

        assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
        assertDestinationPartitionMigrationEvent(msg, destinationRollbackEvent, migration);

        assertReplicaVersionsAndServiceData(msg, migration.getDestination(), migration.getPartitionId(), migration.getDestinationCurrentReplicaIndex());
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
        config.setProperty("hazelcast.logging.type", "log4j");
        return config;
    }

    private MigrationEventCollectingService getService(final Address address) {
        return getNodeEngineImpl(factory.getInstance(address)).getService(MigrationEventCollectingService.SERVICE_NAME);
    }

    private static class MigrationEventCollectingService
            implements MigrationAwareService {

        private static final String SERVICE_NAME = MigrationEventCollectingService.class.getSimpleName();

        private final ConcurrentMap<Integer, Object> data = new ConcurrentHashMap<Integer, Object>();

        private final List<PartitionMigrationEvent> beforeEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<PartitionMigrationEvent> commitEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<PartitionMigrationEvent> rollbackEvents = new ArrayList<PartitionMigrationEvent>();

        private final List<Integer> clearPartitionIds = new ArrayList<Integer>();

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

            if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > BACKUP_COUNT) {
                    data.remove(event.getPartitionId());
                }
            }
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getNewReplicaIndex() > BACKUP_COUNT) {
                    assertNull(data.get(event.getPartitionId()));
                }
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            synchronized (rollbackEvents) {
                rollbackEvents.add(event);
            }

            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > BACKUP_COUNT) {
                    data.remove(event.getPartitionId());
                }
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

        public void clearEvents() {
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

        public boolean containsDataForPartition(final int partitionId) {
            return data.containsKey(partitionId);
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            if (!data.containsKey(event.getPartitionId())) {
                throw new HazelcastException("No data found for " + event);
            }
            return new SampleReplicationOperation();
        }

    }

    private static class SamplePutOperation
            extends AbstractOperation
            implements BackupAwareOperation {
        @Override
        public void run()
                throws Exception {
            MigrationEventCollectingService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return BACKUP_COUNT;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new SampleBackupPutOperation();
        }

        @Override
        public String getServiceName() {
            return MigrationEventCollectingService.SERVICE_NAME;
        }
    }

    private static class SampleBackupPutOperation
            extends AbstractOperation {
        @Override
        public void run()
                throws Exception {
            MigrationEventCollectingService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        @Override
        public String getServiceName() {
            return MigrationEventCollectingService.SERVICE_NAME;
        }
    }

    private static class SampleReplicationOperation
            extends AbstractOperation {

        public SampleReplicationOperation() {
        }

        @Override
        public void run()
                throws Exception {
            // artificial latency!
            randomLatency();
            MigrationEventCollectingService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        private void randomLatency() {
            long duration = (long) (Math.random() * 100);
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(duration) + 100);
        }

        @Override
        public String getServiceName() {
            return MigrationEventCollectingService.SERVICE_NAME;
        }
    }

    private static class RejectMigration
            extends InternalMigrationListener {

        private volatile HazelcastInstance instance;

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            resetInternalMigrationListener(instance); // TODO WE CAN REMOVE THIS
            throw new ExpectedRuntimeException("migration is failed intentionally");
        }

    }

}
