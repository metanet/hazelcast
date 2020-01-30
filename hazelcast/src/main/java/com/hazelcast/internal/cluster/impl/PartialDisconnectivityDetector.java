/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.graph.BronKerboschCliqueFinder;
import com.hazelcast.internal.util.graph.Graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PartialDisconnectivityDetector {
    private final long detectionIntervalMs;
    private final long detectionAlgorithmTimeoutMs;
    private Map<MemberImpl, Set<MemberImpl>> disconnectivityMap = new HashMap<>();
    private long lastUpdated;

    PartialDisconnectivityDetector(long detectionIntervalMs, long detectionAlgorithmTimeoutMs) {
        this.detectionIntervalMs = detectionIntervalMs;
        this.detectionAlgorithmTimeoutMs = detectionAlgorithmTimeoutMs;
    }

    boolean update(MemberImpl member, long timestamp, Collection<MemberImpl> disconnectedMembers) {
        if (timestamp < lastUpdated) {
            return false;
        }

        Set<MemberImpl> currentDisconnectedMembers =  disconnectivityMap.get(member);
        if (currentDisconnectedMembers == null) {
            if (disconnectedMembers.isEmpty()) {
                return false;
            }

            currentDisconnectedMembers = new HashSet<>();
            disconnectivityMap.put(member, currentDisconnectedMembers);
        }

        boolean updated = false;

        for (MemberImpl disconnectedMember : disconnectedMembers) {
            if (currentDisconnectedMembers.add(disconnectedMember)
                    && !disconnectivityMap.getOrDefault(disconnectedMember, emptySet()).contains(member)) {
                lastUpdated = timestamp;
                updated = true;
            }
        }

        if (currentDisconnectedMembers.retainAll(disconnectedMembers)) {
            lastUpdated = timestamp;
            updated = true;
        }

        if (currentDisconnectedMembers.isEmpty()) {
            disconnectivityMap.remove(member);
        }

        return updated;
    }

    boolean shouldCheckPartialDisconnectivity(long timestamp) {
        return !disconnectivityMap.isEmpty() && timestamp - lastUpdated >= detectionIntervalMs;
    }

    Collection<MemberImpl> computeMembersToRemove(MemberImpl master, Collection<MemberImpl> members,
                                                  Map<MemberImpl, Set<MemberImpl>> disconnectivityMap) {
        BiTuple<Collection<MemberImpl>, Graph<MemberImpl>> t = buildConnectivityGraph(disconnectivityMap, members);
        Collection<MemberImpl> fullyConnectedMembers = t.element1;
        if (!fullyConnectedMembers.contains(master)) {
            throw new IllegalStateException("Master: " + master + " is not connected to all members! disconnectivity map: "
                    + disconnectivityMap);
        }

        BronKerboschCliqueFinder<MemberImpl> cliqueFinder = createCliqueFinder(t.element2);
        Collection<Set<MemberImpl>> maxCliques = cliqueFinder.computeMaxCliques();

        if (cliqueFinder.isTimeLimitReached()) {
            throw new IllegalStateException("Partial disconnectivity detection algorithm timed out! disconnectivity map: "
                    + disconnectivityMap);
        } else if (maxCliques.isEmpty()) {
            throw new IllegalStateException("Partial disconnectivity detection algorithm returned no result! "
                    + "disconnectivity map: " + disconnectivityMap);
        }

        Collection<MemberImpl> membersToRemove = new HashSet<>(members);
        membersToRemove.removeAll(fullyConnectedMembers);
        membersToRemove.removeAll(maxCliques.iterator().next());

        return membersToRemove;
    }

    private BiTuple<Collection<MemberImpl>, Graph<MemberImpl>> buildConnectivityGraph(Map<MemberImpl, Set<MemberImpl>>
                                                                                              disconnectivityMap,
                                                                                      Collection<MemberImpl> members) {
        Collection<MemberImpl> fullyConnectedMembers = new HashSet<>(members);
        Collection<MemberImpl> membersWithDisconnectivity = new HashSet<>();

        disconnectivityMap.forEach((key, value) -> {
            membersWithDisconnectivity.add(key);
            membersWithDisconnectivity.addAll(value);
            fullyConnectedMembers.remove(key);
            fullyConnectedMembers.removeAll(value);
        });

        Graph<MemberImpl> graph = new Graph<>();
        membersWithDisconnectivity.forEach(graph::add);

        for (MemberImpl member1 : membersWithDisconnectivity) {
            for (MemberImpl member2 : membersWithDisconnectivity) {
                if (!isDisconnected(disconnectivityMap, member1, member2)) {
                    graph.connect(member1, member2);
                }
            }
        }

        return BiTuple.of(fullyConnectedMembers, graph);
    }

    private boolean isDisconnected(Map<MemberImpl, Set<MemberImpl>> disconnectivityMap, MemberImpl member1, MemberImpl member2) {
        return disconnectivityMap.getOrDefault(member1, emptySet()).contains(member2)
                || disconnectivityMap.getOrDefault(member2, emptySet()).contains(member1);
    }

    private BronKerboschCliqueFinder<MemberImpl> createCliqueFinder(Graph<MemberImpl> graph) {
        return new BronKerboschCliqueFinder<>(graph, detectionAlgorithmTimeoutMs, MILLISECONDS);
    }

    void removeMember(MemberImpl member) {
        disconnectivityMap.remove(member);
        disconnectivityMap.values().forEach(members -> members.remove(member));
    }

    Map<MemberImpl, Set<MemberImpl>> reset() {
        Map<MemberImpl, Set<MemberImpl>> disconnectivityMap = this.disconnectivityMap;
        this.disconnectivityMap = new HashMap<>();
        return disconnectivityMap;
    }

    Map<MemberImpl, Set<MemberImpl>> getDisconnectivityMap() {
        return disconnectivityMap;
    }

}
