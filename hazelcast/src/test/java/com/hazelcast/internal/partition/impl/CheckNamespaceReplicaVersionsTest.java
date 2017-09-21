package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
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

import static com.hazelcast.map.impl.MapService.getObjectNamespace;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CheckNamespaceReplicaVersionsTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 10;
    private static final int BACKUP_COUNT = 2;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private HazelcastInstance instance3;

    private String mapName = randomMapName();
    private ServiceNamespace namespace = getObjectNamespace(mapName);
    private IMap map;
    private PartitionReplicaManager replicaManager;

    @Before
    public void setUp() {
        Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(BACKUP_COUNT);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        factory = createHazelcastInstanceFactory(1);
        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        instance3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, instance2);
        warmUpPartitions(instance1, instance2, instance3);

        map = instance1.getMap(mapName);
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance1);
        replicaManager = partitionService.getReplicaManager();
    }

    @Test
    public void shouldVerifyReplicaVersionsWhenBackupReplicasAreSync() {
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                List<InternalCompletableFuture<Void>> futures = new ArrayList<InternalCompletableFuture<Void>>();

                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    InternalCompletableFuture<Void> future = replicaManager
                            .invokeNamespaceReplicaVersionCheck(partitionId, BACKUP_COUNT, namespace);
                    futures.add(future);
                }

                for (InternalCompletableFuture<Void> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        fail();
                    }
                }
            }
        });
    }

    @Test
    public void shouldNotVerifyReplicaVersionsWhenBackupReplicasAreDirty() {
        dropOperationsFrom(instance1, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        dropOperationsBetween(instance2, instance1, PartitionDataSerializerHook.F_ID, singletonList(PartitionDataSerializerHook.REPLICA_SYNC_REQUEST));
        dropOperationsBetween(instance3, instance1, PartitionDataSerializerHook.F_ID, singletonList(PartitionDataSerializerHook.REPLICA_SYNC_REQUEST));

        for (int i = 0; i < PARTITION_COUNT; i++) {
            map.put(generateKeyForPartition(instance1, i), i);
        }

        resetPacketFiltersFrom(instance1);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                List<InternalCompletableFuture<Void>> futures = new ArrayList<InternalCompletableFuture<Void>>();

                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    InternalCompletableFuture<Void> future = replicaManager
                            .invokeNamespaceReplicaVersionCheck(partitionId, BACKUP_COUNT, namespace);
                    futures.add(future);
                }

                for (InternalCompletableFuture<Void> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        return;
                    }
                }

                fail();
            }
        }, 5);
    }

    @Test
    public void shouldVerifyReplicaVersionsWhenThereIsLessBackupThanRequested() {
        instance3.shutdown();
        waitAllForSafeState(instance1, instance2);

        shouldVerifyReplicaVersionsWhenBackupReplicasAreSync();
    }


}
