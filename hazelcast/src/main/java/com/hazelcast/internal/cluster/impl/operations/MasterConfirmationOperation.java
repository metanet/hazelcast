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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

public class MasterConfirmationOperation extends AbstractClusterOperation implements AllowedDuringPassiveState {

    private long timestamp;

    public MasterConfirmationOperation() {
    }

    public MasterConfirmationOperation(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        final Address endpoint = getCallerAddress();
        if (endpoint == null) {
            return;
        }

        final ClusterServiceImpl clusterService = getService();
        final ILogger logger = getLogger();
        final MemberImpl member = clusterService.getMember(endpoint);
        if (member == null) {
            logger.warning("MasterConfirmation has been received from " + endpoint
                    + ", but it is not a member of this cluster!");
            OperationService operationService = getNodeEngine().getOperationService();
            // TODO [basri] This guy knows me as its master but I am not. I should explicitly tell it to remove me from its cluster.
            // TODO [basri] So, it should remove me from its cluster and update its master address
            operationService.send(new MemberRemoveOperation(clusterService.getThisAddress()), endpoint);
        } else {
            if (clusterService.isMaster()) {
                clusterService.getClusterHeartbeatManager().acceptMasterConfirmation(member, timestamp);
            } else {
                logger.warning(endpoint + " has sent MasterConfirmation, but this node is not master!");
            }
        }
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MASTER_CONFIRM;
    }
}
