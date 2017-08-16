package com.mesosphere.sdk.scheduler.plan;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.scheduler.plan.strategy.Strategy;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mesosphere.sdk.scheduler.plan.PlanUtils.allHaveStatus;
import static com.mesosphere.sdk.scheduler.plan.PlanUtils.anyHaveStatus;


/**
 * A type of {@link Element} which itself is a collection of child {@link Element}s.
 *
 * @param <C> the type of the child elements
 */
public interface ParentElement<C extends Element> extends Element {
    static final Logger LOGGER = LoggerFactory.getLogger(ParentElement.class);

    /**
     * Gets the children of this Element.
     */
    List<C> getChildren();

    /**
     * Gets the {@link Strategy} applied to the deployment of this Element's children.
     */
    Strategy<C> getStrategy();

    @Override
    default boolean interrupt() {
        boolean changed = getStrategy().interrupt();
        notifyObservers();
        return changed;
    }

    @Override
    default boolean proceed() {
        boolean changed = getStrategy().proceed();
        notifyObservers();
        return changed;
    }

    default boolean isInterrupted() {
        return getStrategy().isInterrupted();
    }

    @Override
    default void updateParameters(Map<String, String> parameters) {
        for (C child : getChildren()) {
            child.updateParameters(parameters);
        }
    }

    @Override
    default boolean isEligible(Collection<PodInstanceRequirement> dirtyAssets) {
        return Element.super.isEligible(dirtyAssets) && !isInterrupted();
    }

    /**
     * Updates children.
     */
    @Override
    default void update(Protos.TaskStatus taskStatus) {
        Collection<? extends Element> children = getChildren();
        LOGGER.debug("Updated {} with TaskStatus: {}", getName(), TextFormat.shortDebugString(taskStatus));
        children.forEach(element -> element.update(taskStatus));
    }

    /**
     * Restarts children and returns all children that were modified by the operation.
     */
    @Override
    default Collection<? extends Element> restart() {
        Collection<? extends Element> children = getChildren();
        LOGGER.info("Restarting elements within {}: {}", getName(), children);
        List<Element> modifiedElements = new ArrayList<>();
        for (Element element : children) {
            modifiedElements.addAll(element.restart());
        }
        return modifiedElements;
    }

    /**
     * Force completes children and returns all children that were modified by the operation.
     */
    @Override
    default Collection<? extends Element> forceComplete() {
        Collection<? extends Element> children = getChildren();
        LOGGER.info("Forcing completion of elements within {}: {}", getName(), children);
        List<Element> modifiedElements = new ArrayList<>();
        for (Element element : children) {
            modifiedElements.addAll(element.forceComplete());
        }
        return modifiedElements;
    }

    /**
     * Returns all errors from this {@Link Element} and all its children.
     *
     * @param parentErrors Errors from this {@Link Element} itself.
     * @return a combined list of all errors from the parent and all its children.
     */
    default List<String> getErrors(List<String> parentErrors) {
        List<String> errors = new ArrayList<>();
        errors.addAll(parentErrors);
        Collection<? extends Element> children = getChildren();
        children.forEach(element -> errors.addAll(element.getErrors()));
        return errors;
    }

    @Override
    default Status getStatus() {
        // Ordering matters throughout this method.  Modify with care.
        // Also note that this function MUST NOT call parent.getStatus() as that creates a circular call.

        final List<C> children = getChildren();
        if (children == null) {
            LOGGER.error("Parent element returned null list of children: {}", getName());
            return Status.ERROR;
        }

        final Collection<? extends Element> candidateChildren =
                getStrategy().getCandidates(children, Collections.emptyList());

        Status result;
        if (!getErrors().isEmpty() || anyHaveStatus(Status.ERROR, children)) {
            result = Status.ERROR;
            LOGGER.debug("({} status={}) Elements contain errors.", getName(), result);
        } else if (allHaveStatus(Status.COMPLETE, children)) {
            result = Status.COMPLETE;
            LOGGER.debug("({} status={}) All elements have status: {}",
                    getName(), result, Status.COMPLETE);
        } else if (isInterrupted()) {
            result = Status.WAITING;
            LOGGER.debug("({} status={}) Parent element is interrupted", getName(), result);
        } else if (anyHaveStatus(Status.PREPARED, children)) {
            result = Status.IN_PROGRESS;
            LOGGER.debug("({} status={}) At least one phase has status: {}",
                    getName(), result, Status.PREPARED);
        }  else if (anyHaveStatus(Status.WAITING, candidateChildren)) {
            result = Status.WAITING;
            LOGGER.debug("({} status={}) At least one element has status: {}",
                    getName(), result, Status.WAITING);
        } else if (anyHaveStatus(Status.IN_PROGRESS, candidateChildren)) {
            result = Status.IN_PROGRESS;
            LOGGER.debug("({} status={}) At least one phase has status: {}",
                    getName(), result, Status.IN_PROGRESS);
        } else if (anyHaveStatus(Status.COMPLETE, children) &&
                anyHaveStatus(Status.PENDING, candidateChildren)) {
            result = Status.IN_PROGRESS;
            LOGGER.debug("({} status={}) At least one element has status '{}' and one has status '{}'",
                    getName(), result, Status.COMPLETE, Status.PENDING);
        } else if (anyHaveStatus(Status.COMPLETE, children) &&
                anyHaveStatus(Status.STARTING, candidateChildren)) {
            result = Status.IN_PROGRESS;
            LOGGER.debug("({} status={}) At least one element has status '{}' and one has status '{}'",
                    getName(), result, Status.COMPLETE, Status.STARTING);
        } else if (!candidateChildren.isEmpty() && anyHaveStatus(Status.PENDING, candidateChildren)) {
            result = Status.PENDING;
            LOGGER.debug("({} status={}) At least one element has status: {}",
                    getName(), result, Status.PENDING);
        } else if (anyHaveStatus(Status.WAITING, children)) {
            result = Status.WAITING;
            LOGGER.debug("({} status={}) At least one element has status: {}",
                    getName(), result, Status.WAITING);
        } else if (anyHaveStatus(Status.STARTING, candidateChildren)) {
            result = Status.STARTING;
            LOGGER.debug("({} status={}) At least one element has status '{}'",
                    getName(), result, Status.STARTING);
        } else {
            result = Status.ERROR;
            LOGGER.warn("({} status={}) Unexpected state. children: {}",
                    getName(), result, children);
        }

        return result;
    }
}
