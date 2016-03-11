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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.MigrationDecision.MigrationCallback;
import com.hazelcast.nio.Address;
import com.sun.istack.internal.NotNull;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.partition.impl.MigrationDecision.migrate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MigrationDecisionTest {

    private MigrationCallback callback = mock(MigrationCallback.class);

    @Test
    public void test_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5702), new Address(
                "localhost", 5705), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
    }

    @Test
    public void test_COPY()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), -1, 1);
    }

    @Test
    public void test_MOVE_COPY_BACK_withNullKeepReplicaIndex()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 1, new Address("localhost", 5704), -1, 0);
    }

    @Test
    public void test_MOVE_COPY_BACK_withNullNonNullKeepReplicaIndex()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 1, new Address("localhost", 5704), -1, 0);
    }

    @Test
    public void test_MOVE_COPY_BACK_and_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5702), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 1, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5702), -1, 2);
    }

    @Test
    public void test_SHIFT_UP()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5703), new Address(
                "localhost", 5704), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);

        verify(callback).migrate(null, -1, -1, new Address("localhost", 5703), 2, 1);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), 3, 2);
    }

    @Test
    public void test_SHIFT_UPS_performedBy_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5703), new Address(
                "localhost", 5704), new Address("localhost", 5705), null, null, null};

        migrate(oldAddresses, newAddresses, callback);

        verify(callback).migrate(new Address("localhost", 5704), 3, -1, new Address("localhost", 5705), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5704), -1, 2);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5703), -1, 1);
    }

    @Test
    public void test_MOVE_COPY_BACK_performedAfterKnownNewReplicaOwnerKickedOutOfReplicas()
            throws UnknownHostException {

        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5702), new Address("localhost",
                5701), null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 5, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, new Address("localhost", 5706), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
        verify(callback).migrate(new Address("localhost", 5702), 1, 4, new Address("localhost", 5703), -1, 1);
    }

    @Test
    public void test_MOVE_COPY_BACK_performedBeforeNonConflicting_SHIFT_UP()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5701), null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 4, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, new Address("localhost", 5706), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
    }

    @Test
    public void test_MOVE_toNull()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, null, -1, -1);
    }

    @Test
    public void test_SHIFT_UP_toReplicaIndexWithExistingOwner()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5704), 3, 1);
    }

    @Test
    public void test_MOVE_performedAfter_SHIFT_UP_toReplicaIndexWithExistingOwnerKicksItOutOfCluster()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5702), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5704), 3, 1);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5702), -1, 0);
    }

    @Test
    public void test_SHIFT_UP_multipleTimes()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5702), null, new Address("localhost",
                5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5702), new Address("localhost", 5703), new Address(
                "localhost", 5704), null, null, null, null};

        migrate(oldAddresses, newAddresses, callback);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5703), 2, 1);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), 3, 2);
    }

    @Test
    public void testRandom()
            throws UnknownHostException {
        for (int i = 0; i < 100; i++) {
            testRandom(3);
            testRandom(4);
            testRandom(5);
        }
    }

    private void testRandom(int initialLen)
            throws java.net.UnknownHostException {
        Address[] oldAddresses = new Address[InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < initialLen; i++) {
            oldAddresses[i] = newAddress(5000 + i);
        }

        Address[] newAddresses = Arrays.copyOf(oldAddresses, oldAddresses.length);
        int newLen = (int) (Math.random() * (oldAddresses.length - initialLen + 1));
        for (int i = 0; i < newLen; i++) {
            newAddresses[i + initialLen] = newAddress(6000 + i);
        }

        shuffle(newAddresses, initialLen + newLen);

        migrate(oldAddresses, newAddresses, callback);
    }

    private void shuffle(Address[] array, int len) {
        int index;
        Address temp;
        Random random = new Random();
        for (int i = len - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    @NotNull
    private Address newAddress(int port)
            throws java.net.UnknownHostException {
        return new Address("localhost", port);
    }

}
