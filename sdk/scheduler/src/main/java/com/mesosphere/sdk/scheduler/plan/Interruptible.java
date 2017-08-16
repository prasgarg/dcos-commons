package com.mesosphere.sdk.scheduler.plan;

/**
 * Interface for plan components which may be flagged as interrupted. The interrupt call is an override on top of any
 * internal status the object may otherwise have.
 */
public interface Interruptible {

    /**
     * A call to interrupt indicates that an {@link Interruptible} should not continue work beyond the current point,
     * until {@link #proceed()} is called. This call has no effect if the object is already interrupted.
     *
     * @return whether the call modified state (i.e. if the object was proceeding before the call)
     */
    boolean interrupt();

    /**
     * A call to proceed indicates that an {@link Interruptible} may cancel a previous {@link #interrupt()} call and
     * resume with any pending work. This call has no effect if the object is already proceeding.
     *
     * @return whether the call modified state (i.e. if the object was interrupted before the call)
     */
    boolean proceed();

    /**
     * Indicates whether the object is interrupted or not.
     *
     * @return {@code true} if the object is interrupted, {@code false} otherwise
     */
    boolean isInterrupted();

}
