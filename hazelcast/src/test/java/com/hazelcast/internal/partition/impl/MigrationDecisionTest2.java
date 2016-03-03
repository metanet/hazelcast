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
import com.hazelcast.nio.Address;
import com.sun.istack.internal.NotNull;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.partition.impl.MigrationDecision.migrate;

public class MigrationDecisionTest2 {

    @Test
    public void test()
            throws UnknownHostException {

        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5702), new Address("localhost",
                5701), null};

        migrate(oldAddresses, newAddresses);

    }

    @Test
    public void test2()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5701), null, null};

        migrate(oldAddresses, newAddresses);
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

    private static void testRandom(int initialLen) throws java.net.UnknownHostException {
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

        migrate(oldAddresses, newAddresses);
    }

    private static void shuffle(Address[] array, int len) {
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
    private static Address newAddress(int port) throws java.net.UnknownHostException {
        return new Address("localhost", port);
    }
}
