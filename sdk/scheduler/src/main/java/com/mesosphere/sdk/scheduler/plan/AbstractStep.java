package com.mesosphere.sdk.scheduler.plan;

import com.google.common.annotations.VisibleForTesting;
import com.mesosphere.sdk.scheduler.DefaultObservable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

/**
 * Provides a default implementation of commonly-used {@link Step} logic.
 */
public abstract class AbstractStep extends DefaultObservable implements Step {

    /**
     * Non-static to ensure that we inherit the names of subclasses.
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final UUID id = UUID.randomUUID();
    private final String name;

    private final Object statusLock = new Object();
    private Status status;
    private boolean interrupted;

    protected AbstractStep(String name, Status status) {
        this.name = name;
        this.status = status;
        this.interrupted = false;
        logger.info("{}: Initialized status to: {}", getName(), status);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Status getStatus() {
        synchronized (statusLock) {
            if (interrupted && (status == Status.PENDING || status == Status.PREPARED)) {
                return Status.WAITING;
            }
            return status;
        }
    }

    /**
     * Updates the status setting and logs the outcome. Should only be called either by tests, by {@code this}, or by
     * subclasses.
     *
     * @param newStatus the new status to be set
     * @return the prior status before the call
     */
    protected Status setStatus(Status newStatus) {
        Status oldStatus;
        synchronized (statusLock) {
            oldStatus = status;
            status = newStatus;
            if (oldStatus != newStatus) {
                logger.info("{}: changed status from: {} to: {} (interrupted={})",
                        getName(), oldStatus, newStatus, interrupted);
            } else {
                logger.info("{}: no change to status: {} (interrupted={})",
                        getName(), oldStatus, newStatus, interrupted);
            }
        }
        // Just in case, avoid possibility of deadlocks by calling out from outside the lock:
        if (oldStatus != newStatus) {
            notifyObservers();
        }
        return oldStatus;
    }

    /**
     * Calls {@link #setStatus(Status)} with the provided status and returns {@code this} if the call resulted in a
     * modification.
     */
    @VisibleForTesting
    protected Collection<? extends Element> setStatusGetChanged(Status newStatus) {
        return setStatus(newStatus) == newStatus
                ? Collections.emptyList() // no change
                : Collections.singleton(this); // changed
    }

    /**
     * Updates the interrupted bit with the specified value and returns {@code this} if the interrupted bit was changed
     * as a result.
     */
    private boolean setInterruptedGetChanged(boolean interrupt) {
        boolean wasInterrupted;
        synchronized (statusLock) {
            wasInterrupted = interrupted;
            interrupted = interrupt;
        }
        return wasInterrupted != interrupt;
    }

    @Override
    public boolean interrupt() {
        return setInterruptedGetChanged(true);
    }

    @Override
    public boolean proceed() {
        return setInterruptedGetChanged(false);
    }

    @Override
    public boolean isInterrupted() {
        synchronized (statusLock) {
            return interrupted;
        }
    }

    @Override
    public Collection<? extends Element> restart() {
        logger.warn("Restarting step: '{} [{}]'", getName(), getId());
        return setStatusGetChanged(Status.PENDING);
    }

    @Override
    public Collection<? extends Element> forceComplete() {
        logger.warn("Forcing completion of step: '{} [{}]'", getName(), getId());
        return setStatusGetChanged(Status.COMPLETE);
    }

    @Override
    public String toString() {
        // Avoid using ReflectionToStringBuilder: It pulls in all of our observers, which leads to very long output...
        return String.format("%s(name=%s,status=%s)", getClass().getSimpleName(), getName(), getStatus());
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
