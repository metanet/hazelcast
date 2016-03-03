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

import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.partition.impl.InternalPartitionImpl.getReplicaIndex;

/**
 * TODO: Javadoc Pending...
 *
 */
class MigrationDecision {

    interface MigrationCallback {
        void migrate(Address currentOwner, Address newOwner, int replicaIndex, MigrationType type, int keepReplicaIndex);
    }

    static void migrate(Address[] oldAddresses, Address[] newAddresses, MigrationCallback callback) {

        Address[] state = new Address[oldAddresses.length];
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);

        log("INITIAL STATE: " + Arrays.toString(state));
        log("FINAL STATE: " + Arrays.toString(newAddresses));
        if (fixCycle(oldAddresses, newAddresses)) {
            log("cycle fixed");
        }
        log("EXPECTED STATE: " + Arrays.toString(newAddresses));

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {

            if (newAddresses[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    log("new address is null at index: " + currentIndex);
                    callback.migrate(state[currentIndex], null, currentIndex, MigrationType.MOVE, -1);
                    state[currentIndex] = null;
                }
                currentIndex++;
                continue;
            }

            if (state[currentIndex] == null) {
                log("COPY " + newAddresses[currentIndex] + " to index: " + currentIndex);
                callback.migrate(null, newAddresses[currentIndex], currentIndex, MigrationType.COPY, -1);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            if (newAddresses[currentIndex].equals(state[currentIndex])) {
                log("Addresses are same on index: " + currentIndex);
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(newAddresses, state[currentIndex]) == -1
                    && getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                log("MOVE " + newAddresses[currentIndex] + " to index: " + currentIndex);
                callback.migrate(state[currentIndex], newAddresses[currentIndex], currentIndex, MigrationType.MOVE, -1);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            // IT IS A MOVE COPY BACK
            if (getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                int keepReplicaIndex = getReplicaIndex(newAddresses, state[currentIndex]);
                log("MOVE_COPY_BACK " + newAddresses[currentIndex] + " to index: " + currentIndex
                        + " with keepReplicaIndex: " + keepReplicaIndex);

                callback.migrate(state[currentIndex], newAddresses[currentIndex], currentIndex,
                        MigrationType.MOVE, keepReplicaIndex);

                state[keepReplicaIndex] = state[currentIndex];
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            Address target = newAddresses[currentIndex];
            while (true) {
                int j = getReplicaIndex(state, target);

                if (j == -1) {
                    throw new AssertionError(target + " is not present in " + Arrays.toString(state));

                } else if (newAddresses[j] == null) {
                    log("SHIFT UP " + state[j] + " from old addresses index: " + j + " to index: " + currentIndex);
                    callback.migrate(null, state[j], currentIndex, MigrationType.MOVE, -1);
                    state[currentIndex] = state[j];
                    break;
                } else if (getReplicaIndex(state, newAddresses[j]) == -1) { //
                    log("MOVE2 " + newAddresses[j] + " to index: " + j);
                    callback.migrate(state[j], newAddresses[j], j, MigrationType.MOVE, -1);
                    state[j] = newAddresses[j];
                    break;
                } else {
                    // newAddresses[j] is also present in old partition replicas
                    target = newAddresses[j];
                }
            }

            log("STATE: " + Arrays.toString(state));
            Set<Address> verify = new HashSet<Address>();
            for (Address s : state) {
                if (s != null) {
                    assert verify.add(s);
                }
            }
        }
        assert Arrays.equals(state, newAddresses) : Arrays.toString(state) + " vs " + Arrays.toString(newAddresses);
    }

    static boolean isCyclic(Address[] oldReplicas, Address[] newReplicas) {
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                return true;
            }
        }

        return false;
    }

    static boolean fixCycle(Address[] oldReplicas, Address[] newReplicas) {
        boolean cyclic = false;
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                fixCycle(oldReplicas, newReplicas, i);
                cyclic = true;
            }
        }

        return cyclic;
    }

    private static boolean isCyclic(Address[] oldReplicas, Address[] newReplicas, int index) {
        final Address newOwner = newReplicas[index];
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[index]);
            if (nextIndex == -1) {
                return false;
            } else if (newOwner.equals(oldReplicas[nextIndex])) {
                return true;
            } else {
                index = nextIndex;
            }
        }
    }

    private static void fixCycle(Address[] oldReplicas, Address[] newReplicas, int index) {
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

    private static final boolean TRACE = false;
    private static void log(String x) {
        if (TRACE) {
            System.out.println(x);
        }
    }

}
