/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class/*, ParallelTest.class*/})
//@Ignore // related issue https://github.com/hazelcast/hazelcast/issues/5444
public class MigrationAwareServiceTest extends HazelcastTestSupport {

    private static final String BACKUP_COUNT_PROP = "backups.count";
    private static final int PARTITION_COUNT = 111;
    private static final int PARALLEL_REPLICATIONS = 10;
    private static final int BACKUP_SYNC_INTERVAL = 1;
    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameterized.Parameters(name = "backupCount:{0},nodeCount:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {1, 2}, {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3}, {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4}, {3, InternalPartition.MAX_REPLICA_COUNT},
        });
    }

    private TestHazelcastInstanceFactory factory;

    @Parameterized.Parameter(0)
    public int backupCount;

    @Parameterized.Parameter(1)
    public int nodeCount;

    private boolean antiEntropyEnabled = false;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(10);
    }

    @Test
    public void testPartitionData_whenNodesStartedSequentially() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSize(backupCount);

        for (int i = 1; i <= nodeCount; i++) {
            startNodes(config, 1);
            assertSize(backupCount);
            assertReplicaVersions(backupCount);
        }
    }

    @Test
    public void testPartitionData_whenNodesStartedParallel() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSize(backupCount);

        startNodes(config, nodeCount);
        assertSize(backupCount);
        assertReplicaVersions(backupCount);
    }

    @Test
    public void testPartitionData_whenBackupNodesTerminated() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount);
        warmUpPartitions(factory.getAllHazelcastInstances());

        fill(hz);
        assertSize(backupCount);

        terminateNodes(backupCount);
        assertSize(backupCount);
    }

    @Test
    public void testPartitionData_whenBackupNodesStartedTerminated() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSize(backupCount);

        while (factory.getAllHazelcastInstances().size() < (nodeCount + 1)) {
            startNodes(config, backupCount + 1);
            assertSize(backupCount);

            terminateNodes(backupCount);
            assertSize(backupCount);
        }
    }

    @Test
    public void testPartitionData_withAntiEntropy() throws InterruptedException {
        antiEntropyEnabled = true;

        HazelcastInstance[] instances = factory.newInstances(getConfig(backupCount), nodeCount);
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
            cm.setPacketFilter(new BackupPacketFilter(node.getSerializationService(), BACKUP_BLOCK_RATIO));
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fill(instance);
        }

        assertSize(backupCount);
        assertReplicaVersions(backupCount);
    }

    private void fill(HazelcastInstance hz) {
        NodeEngine nodeEngine = getNode(hz).nodeEngine;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            nodeEngine.getOperationService().invokeOnPartition(null, new SamplePutOperation(), i);
        }
    }

    private void startNodes(final Config config, int count) throws InterruptedException {
        if (count == 1) {
            factory.newHazelcastInstance(config);
        } else {
            final CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                new Thread() {
                    public void run() {
                        factory.newHazelcastInstance(config);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    private void terminateNodes(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        Collections.shuffle(instances);

        if (count == 1) {
            TestUtil.terminateInstance(instances.get(0));
        } else {
            int min = Math.min(count, instances.size());
            final CountDownLatch latch = new CountDownLatch(min);

            for (int i = 0; i < min; i++) {
                final HazelcastInstance hz = instances.get(i);
                new Thread() {
                    public void run() {
                        TestUtil.terminateInstance(hz);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    private void assertSize(final int backupCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                int expectedSize = PARTITION_COUNT * Math.min(backupCount + 1, instances.size());

                int total = 0;
                StringBuilder s = new StringBuilder();
                for (HazelcastInstance hz : instances) {
                    SampleMigrationAwareService service = getService(hz);
                    total += service.size();

                    Node node = getNode(hz);
                    InternalPartitionService partitionService = node.getPartitionService();
                    InternalPartition[] partitions = partitionService.getInternalPartitions();

                    s.append("\nLEAK ").append(node.getThisAddress()).append("\n");
                    for (Integer p : service.data.keySet()) {
                        int replicaIndex = partitions[p].getReplicaIndex(node.getThisAddress());
                        if (replicaIndex < 0 || replicaIndex > backupCount) {
                            s.append(p).append(": ").append(replicaIndex).append("\n");
                        }
                    }
                    s.append("\n");

                    s.append("MISSING ").append(node.getThisAddress()).append("\n");
                    for (InternalPartition partition : partitions) {
                        int replicaIndex = partition.getReplicaIndex(node.getThisAddress());
                        if (replicaIndex >= 0 && replicaIndex <= backupCount) {
                            if (!service.data.containsKey(partition.getPartitionId())) {
                                s.append(partition.getPartitionId()).append(": ").append(replicaIndex).append("\n");
                            }
                        }
                    }
                }
                assertEquals(s.toString(), expectedSize, total);
            }
        }, 100);
    }

    private void assertReplicaVersions(int backupCount) throws InterruptedException {
        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final Map<Address, Node> nodes = new HashMap<Address, Node>();

        backupCount = Math.min(backupCount, instances.size() - 1);

        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            nodes.put(node.getThisAddress(), node);
        }

        for (Node node : nodes.values()) {
            InternalPartitionService partitionService = node.getPartitionService();
            InternalPartition[] partitions = partitionService.getInternalPartitions();

            for (InternalPartition partition : partitions) {
                if (partition.isLocal()) {
                    long[] replicaVersions = getReplicaVersions(node, partition.getPartitionId());
                    for (int replica = 1; replica <= backupCount; replica++) {
                        Address address = partition.getReplicaAddress(replica);
                        assertNotNull(partition.toString(), address);
                        Node backupNode = nodes.get(address);
                        assertNotNull(backupNode);

                        long[] backupReplicaVersions = getReplicaVersions(backupNode, partition.getPartitionId());
                        for (int i = replica - 1; i < backupCount; i++) {
                            assertEquals("Owner: " + node.getThisAddress() + ", Backup: " + address
                                    + ", PartitionId: " + partition.getPartitionId() + ", Replica: " + (i + 1),
                                    replicaVersions[i], backupReplicaVersions[i]);
                        }
                    }
                }
            }
        }
    }

    private SampleMigrationAwareService getService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine.getService(SampleMigrationAwareService.SERVICE_NAME);
    }

    private Config getConfig(int backupCount) {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true).setName(SampleMigrationAwareService.SERVICE_NAME)
                .setClassName(SampleMigrationAwareService.class.getName())
                .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));

        config.getServicesConfig().addServiceConfig(serviceConfig);
        config.setProperty(GroupProperty.PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config.setProperty(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS, String.valueOf(1));
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL, String.valueOf(BACKUP_SYNC_INTERVAL));

        int parallelReplications = antiEntropyEnabled ? PARALLEL_REPLICATIONS : 0;
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS, String.valueOf(parallelReplications));
        return config;
    }

    private static class SampleMigrationAwareService implements ManagedService, MigrationAwareService {

        private static final String SERVICE_NAME = "SampleMigrationAwareService";

        private final ConcurrentMap<Integer, Object> data = new ConcurrentHashMap<Integer, Object>();

        private volatile int backupCount;

        private volatile ILogger logger;

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            backupCount = Integer.parseInt(properties.getProperty(BACKUP_COUNT_PROP, "1"));
            logger = nodeEngine.getLogger(getClass());
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        int size() {
            return data.size();
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
//            logger.info(event.toString() + "\n");
            if (event.getReplicaIndex() > backupCount) {
                return null;
            }
            if (!data.containsKey(event.getPartitionId())) {
                throw new HazelcastException("No data found for " + event);
            }
            return new SampleReplicationOperation();
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
//            logger.info("COMMIT: " + event.toString() + "\n");
            if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > backupCount) {
                    data.remove(event.getPartitionId());
                }
            }
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getNewReplicaIndex() > backupCount) {
                    assertNull(data.get(event.getPartitionId()));
                }
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
//            logger.info("ROLLBACK: " + event.toString() + "\n");
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > backupCount) {
                    data.remove(event.getPartitionId());
                }
            }
        }

        @Override
        public void clearPartitionReplica(int partitionId) {
//            logger.info("CLEAR: " + partitionId);
//            new UnsupportedOperationException().printStackTrace();
            data.remove(partitionId);
        }
    }

    private static class SamplePutOperation extends AbstractOperation implements BackupAwareOperation {
        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            SampleMigrationAwareService service = getService();
            return service.backupCount;
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
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class SampleBackupPutOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class SampleReplicationOperation extends AbstractOperation {

        public SampleReplicationOperation() {
        }

        @Override
        public void run() throws Exception {
            // artificial latency!
            randomLatency();
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        private void randomLatency() {
            long duration = (long) (Math.random() * 100);
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(duration) + 100);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class BackupPacketFilter implements PacketFilter {
        final SerializationService serializationService;
        final float ratio;

        public BackupPacketFilter(SerializationService serializationService, float ratio) {
            this.serializationService = serializationService;
            this.ratio = ratio;
        }

        @Override
        public boolean allow(Packet packet, Address endpoint) {
            return !packet.isFlagSet(Packet.FLAG_OP) || allowOperation(packet);
        }

        private boolean allowOperation(Packet packet) {
            try {
                ObjectDataInput input = serializationService.createObjectDataInput(packet);
                boolean identified = input.readBoolean();
                if (identified) {
                    int factory = input.readInt();
                    int type = input.readInt();
                    boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
                    return !isBackup || Math.random() > ratio;
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return true;
        }
    }
}
