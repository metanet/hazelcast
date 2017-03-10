package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;

// TODO [basri] ADD JAVADOC
public class ExplicitSuspicionOp extends AbstractClusterOperation {

    private MembersViewMetadata membersViewMetadata;

    private Address suspectedAddress;

    public ExplicitSuspicionOp() {
    }

    public ExplicitSuspicionOp(MembersViewMetadata membersViewMetadata, Address suspectedAddress) {
        this.membersViewMetadata = membersViewMetadata;
        this.suspectedAddress = suspectedAddress;
    }

    @Override
    public void run() throws Exception {
        getLogger().info("Received suspicion request for: " + suspectedAddress + " from: " + getCallerAddress());

        if (!isCallerValid(getCallerAddress())) {
            return;
        }
        
        final ClusterServiceImpl clusterService = getService();
        clusterService.handleExplicitSuspicion(membersViewMetadata, suspectedAddress);
    }

    private boolean isCallerValid(Address caller) {
        ILogger logger = getLogger();

        if (caller == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring suspicion request of " + suspectedAddress + ", because sender is local or not known.");
            }
            return false;
        }

        if (!suspectedAddress.equals(caller)) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring suspicion request of " + suspectedAddress + ", because sender must be either itself or master. "
                        + "Sender: " + caller);
            }
            return false;
        }
        return true;
    }

    @Override
    public int getId() {
        return EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(membersViewMetadata);
        out.writeObject(suspectedAddress);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        membersViewMetadata = in.readObject();
        suspectedAddress = in.readObject();
    }

}
