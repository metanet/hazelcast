/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Joiner;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

import java.util.Set;

import static java.lang.String.format;

public class NodeMulticastListener implements MulticastListener {

    private final Node node;
    private final ILogger logger;
    private ConfigCheck ourConfig;

    public NodeMulticastListener(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeMulticastListener.class.getName());
        this.ourConfig = node.createConfigCheck();
    }

    @Override
    public void onMessage(Object msg) {
        if (!isValidJoinMessage(msg)) {
            logDroppedMessage(msg);
            return;
        }

        JoinMessage joinMessage = (JoinMessage) msg;
        if (node.isRunning() && node.joined()) {
            handleActiveAndJoined(joinMessage);
        } else {
            handleNotActiveOrNotJoined(joinMessage);
        }
    }

    private void logDroppedMessage(Object msg) {
        if (logger.isFineEnabled()) {
            logger.fine("Dropped: " + msg);
        }
    }

    private void handleActiveAndJoined(JoinMessage joinMessage) {
        if (!(joinMessage instanceof JoinRequest)) {
            logDroppedMessage(joinMessage);
            return;
        }

        if (node.isMaster()) {
            JoinMessage response = new JoinMessage(Packet.VERSION, node.getBuildInfo().getBuildNumber(), node.getVersion(),
                    node.getThisAddress(), node.getThisUuid(), node.isLiteMember(), node.createConfigCheck());
            node.multicastService.send(response);
        } else if (isMasterNode(joinMessage.getAddress()) && !checkMasterUuid(joinMessage.getUuid())) {
            String message = "New join request has been received from current master. "
                    + "Removing " + node.getMasterAddress();
            logger.warning(message);
            // TODO [basri] I am a slave and the master I follow says that it is gone. So, I can remove it instead of suspecting it.
            // TODO [basri] I can only make a local suspicion. Since it is a multicast message, probably other nodes will eventually suspect as well.
            // TODO [basri] Since I am not the master, I cannot change other members' opinions about suspicions.
            node.getClusterService().removeAddress(node.getMasterAddress(), message);
        }
    }

    private void handleNotActiveOrNotJoined(JoinMessage joinMessage) {
        if (isJoinRequest(joinMessage)) {
            Joiner joiner = node.getJoiner();
            if (joiner instanceof MulticastJoiner) {
                MulticastJoiner multicastJoiner = (MulticastJoiner) joiner;
                multicastJoiner.onReceivedJoinRequest((JoinRequest) joinMessage);
            } else {
                logDroppedMessage(joinMessage);
            }
        } else {
            Address address = joinMessage.getAddress();
            if (node.getJoiner().isBlacklisted(address)) {
                logDroppedMessage(joinMessage);
                return;
            }

            if (!node.joined() && node.getMasterAddress() == null) {
                ClusterJoinManager clusterJoinManager = node.getClusterService().getClusterJoinManager();
                //todo: why are we making a copy here of address?
                Address masterAddress = new Address(joinMessage.getAddress());
                clusterJoinManager.setMasterAddress(masterAddress);
            } else {
                logDroppedMessage(joinMessage);
            }
        }
    }

    private boolean isJoinRequest(JoinMessage joinMessage) {
        return joinMessage instanceof JoinRequest;
    }

    private void logJoinMessageDropped(String masterHost) {
        if (logger.isFineEnabled()) {
            logger.fine(format(
                    "JoinMessage from %s is dropped because its sender is not a trusted interface", masterHost));
        }
    }

    private boolean isJoinMessage(Object msg) {
        return msg != null && msg instanceof JoinMessage && !(msg instanceof SplitBrainJoinMessage);
    }

    private boolean isValidJoinMessage(Object msg) {
        if (!isJoinMessage(msg)) {
            return false;
        }

        JoinMessage joinMessage = (JoinMessage) msg;

        if (isMessageToSelf(joinMessage)) {
            return false;
        }

        ConfigCheck theirConfig = joinMessage.getConfigCheck();
        if (!ourConfig.isSameGroup(theirConfig)) {
            return false;
        }
        return true;
    }

    private boolean isMessageToSelf(JoinMessage joinMessage) {
        Address thisAddress = node.getThisAddress();
        return thisAddress == null || thisAddress.equals(joinMessage.getAddress());
    }

    private boolean isMasterNode(Address address) {
        return address.equals(node.getMasterAddress());
    }

    private boolean checkMasterUuid(String uuid) {
        Member masterMember = getMasterMember(node.getClusterService().getMembers());
        return masterMember == null || masterMember.getUuid().equals(uuid);
    }

    private Member getMasterMember(Set<Member> members) {
        if (members.isEmpty()) {
            return null;
        }

        return members.iterator().next();
    }
}
