package com.hazelcast.spi;

import java.util.concurrent.TimeUnit;

/**
 * An interface that can be implemented by SPI services to participate in graceful shutdown process, such as moving
 * their internal data to another node or releasing their allocated resources gracefully.
 */
public interface GracefulShutdownAwareService {

    /**
     * A hook method that's called during graceful shutdown to provide safety for data managed by this service.
     * Shutdown process is blocked until this method returns or shutdown timeouts. If this method does not
     * return in time, then it's considered as failed to gracefully shutdown.
     *
     * @param timeout timeout for graceful shutdown
     * @param unit time unit
     * @return true if graceful shutdown is successful, false otherwise
     */
    boolean onShutdown(long timeout, TimeUnit unit);

}
