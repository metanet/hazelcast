package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.nio.tcp.OperationPacketFilter;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.util.collection.IntHashSet;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.LEN;
import static com.hazelcast.test.HazelcastTestSupport.getNode;

final class PacketFiltersUtil {

    private PacketFiltersUtil() {

    }

    static void resetPacketFiltersFrom(HazelcastInstance instance) {
        Node node = getNode(instance);
        FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
        cm.removeDroppingPacketFilter();
        cm.removeDelayingPacketFilter();
    }

    static void delayOperationsFrom(HazelcastInstance instance, int... opTypes) {
        Node node = getNode(instance);
        FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
        PacketFilter packetFilter = new ClusterOperationPacketFilter(node.getSerializationService(), opTypes);
        cm.setDelayingPacketFilter(packetFilter, 500, 5000);
    }

    static void dropOperationsFrom(HazelcastInstance instance, int... opTypes) {
        Node node = getNode(instance);
        FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
        PacketFilter packetFilter = new ClusterOperationPacketFilter(node.getSerializationService(), opTypes);
        cm.setDroppingPacketFilter(packetFilter);
    }

    static class ClusterOperationPacketFilter extends OperationPacketFilter {

        final IntHashSet types = new IntHashSet(LEN, 0);

        ClusterOperationPacketFilter(InternalSerializationService serializationService, int...typeIds) {
            super(serializationService);
            for (int id : typeIds) {
                types.add(id);
            }
        }

        @Override
        protected boolean allowOperation(int factory, int type) {
            boolean drop = factory == F_ID && types.contains(type);
            return !drop;
        }
    }

}
