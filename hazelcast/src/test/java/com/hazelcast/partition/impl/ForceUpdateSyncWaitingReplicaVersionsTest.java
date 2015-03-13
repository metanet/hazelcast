package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static com.hazelcast.partition.impl.PartitionReplicaAssignmentReason.ASSIGNMENT;
import static com.hazelcast.partition.impl.PartitionReplicaAssignmentReason.MEMBER_REMOVED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ForceUpdateSyncWaitingReplicaVersionsTest {

    @Mock
    private InternalPartitionService partitionService;

    @Mock
    private NodeEngineImpl nodeEngine;

    @Mock
    private PartitionAwareService partitionAwareService;

    @Before
    public void before() {
        when(nodeEngine.getService("partitionService")).thenReturn(partitionService);
        when(nodeEngine.getLogger(ForceUpdateSyncWaitingReplicaVersions.class)).thenReturn(mock(ILogger.class));
        when(nodeEngine.getServices(PartitionAwareService.class)).thenReturn(Arrays.asList(partitionAwareService));
    }

    @Test
    public void test_partitionLostEventDispatched_onFirstNodeFailure()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};
        final long[] expectedVersions = {6, 5, 4, 3, 2, 1};
        final int partitionId = 1;
        final InternalPartitionLostEvent expectedEvent = new InternalPartitionLostEvent(partitionId, 0, null);

        testForceUpdateSyncWaitingReplicaVersions(versions, expectedVersions, partitionId, MEMBER_REMOVED, expectedEvent);
    }

    @Test
    public void test_partitionLostEventDispatched_onSecondFirstNodeFailure()
            throws Exception {
        final long[] versions = {-1, 5, 4, 3, 2, 1};
        final long[] expectedVersions = {5, 5, 4, 3, 2, 1};
        final int partitionId = 1;
        final InternalPartitionLostEvent expectedEvent = new InternalPartitionLostEvent(partitionId, 1, null);

        testForceUpdateSyncWaitingReplicaVersions(versions, expectedVersions, partitionId, MEMBER_REMOVED, expectedEvent);
    }

    @Test
    public void test_partitionLostEventNotDispatched_onAssignment()
            throws Exception {
        final long[] versions = {0, -1, -1, 3, 2, 1};
        final long[] expectedVersions = {0, 0, 0, 3, 2, 1};
        final int partitionId = 1;

        testForceUpdateSyncWaitingReplicaVersions(versions, expectedVersions, partitionId, ASSIGNMENT, null);
    }

    private void testForceUpdateSyncWaitingReplicaVersions(final long[] versions, final long[] expectedVersions,
                                                           final int partitionId, final PartitionReplicaAssignmentReason reason,
                                                           final InternalPartitionLostEvent expectedEvent)
            throws Exception {
        final ForceUpdateSyncWaitingReplicaVersions operation = createOperation(partitionId, reason);
        when(partitionService.getPartitionReplicaVersions(partitionId)).thenReturn(versions);

        operation.run();

        assertThat(versions, equalTo(expectedVersions));
        if (expectedEvent != null) {
            verify(partitionAwareService).onPartitionLostEvent(expectedEvent);
        } else {
            verify(partitionAwareService, never()).onPartitionLostEvent(any(InternalPartitionLostEvent.class));
        }
    }

    private ForceUpdateSyncWaitingReplicaVersions createOperation(final int partitionId,
                                                                  final PartitionReplicaAssignmentReason reason) {
        final ForceUpdateSyncWaitingReplicaVersions operation = new ForceUpdateSyncWaitingReplicaVersions(reason);
        operation.setPartitionId(partitionId);
        operation.setNodeEngine(nodeEngine);
        operation.setServiceName("partitionService");
        return operation;
    }

}
