package com.mesosphere.sdk.scheduler.uninstall;

import com.mesosphere.sdk.scheduler.plan.PodInstanceRequirement;
import com.mesosphere.sdk.scheduler.plan.Status;
import com.mesosphere.sdk.state.StateStore;
import org.apache.mesos.SchedulerDriver;

import java.util.Optional;

/**
 * Step which implements the deregistering of a framework.
 */
public class DeregisterStep extends UninstallStep {

    private SchedulerDriver schedulerDriver;
    private StateStore stateStore;

    /**
     * Creates a new instance with initial {@code status}. The {@link SchedulerDriver} must be
     * set separately.
     */
    DeregisterStep(StateStore stateStore) {
        super("deregister", Status.PENDING);
        this.stateStore = stateStore;
    }

    /**
     *
     * @param schedulerDriver Must be set before call to {@link #start()}
     */
    void setSchedulerDriver(SchedulerDriver schedulerDriver) {
        this.schedulerDriver = schedulerDriver;
    }

    @Override
    public Optional<PodInstanceRequirement> start() {
        logger.info("Stopping SchedulerDriver...");
        // Remove the framework ID before unregistering
        stateStore.clearFrameworkId();
        // Unregisters the framework in addition to stopping the SchedulerDriver thread:
        // Calling with failover == false causes Mesos to teardown the framework.
        schedulerDriver.stop(false);
        logger.info("Deleting service root path for framework...");
        stateStore.clearAllData();
        logger.info("Finished deleting service root path for framework");
        setStatus(Status.COMPLETE);
        return Optional.empty();
    }
}
