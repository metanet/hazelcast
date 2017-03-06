package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

// TODO [basri] ADD JAVADOC
public class ExplicitSuspicionOperation extends AbstractClusterOperation {


    private Address suspectedAddress;

    private boolean destroyConnection;

    public ExplicitSuspicionOperation(Address suspectedAddress, boolean destroyConnection) {
        this.suspectedAddress = suspectedAddress;
        this.destroyConnection = destroyConnection;
    }

    public ExplicitSuspicionOperation() {
    }

    @Override
    public void run() throws Exception {
        getLogger().info("Received suspicion request for: " + suspectedAddress + " from: " + getCallerAddress());

        if (!isCallerValid(getCallerAddress())) {
            return;
        }
        
        final ClusterServiceImpl clusterService = getService();
        clusterService.suspectAddress(suspectedAddress, "explicit suspicion", destroyConnection);
    }

    private boolean isCallerValid(Address caller) {
        ClusterServiceImpl clusterService = getService();
        ILogger logger = getLogger();

        if (caller == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring suspicion request of " + suspectedAddress + ", because sender is local or not known.");
            }
            return false;
        }

        if (!suspectedAddress.equals(caller) && !caller.equals(clusterService.getMasterAddress())) {
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
        return ClusterDataSerializerHook.EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(suspectedAddress);
        out.writeBoolean(destroyConnection);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        suspectedAddress = in.readObject();
        destroyConnection = in.readBoolean();
    }

}
