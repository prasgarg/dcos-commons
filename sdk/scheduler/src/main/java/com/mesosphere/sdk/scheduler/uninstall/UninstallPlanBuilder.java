package com.mesosphere.sdk.scheduler.uninstall;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.sdk.dcos.SecretsClient;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.ResourceUtils;
import com.mesosphere.sdk.offer.evaluate.security.SecretNameGenerator;
import com.mesosphere.sdk.scheduler.SchedulerFlags;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.DefaultPlan;
import com.mesosphere.sdk.scheduler.plan.Phase;
import com.mesosphere.sdk.scheduler.plan.Plan;
import com.mesosphere.sdk.scheduler.plan.Status;
import com.mesosphere.sdk.scheduler.plan.Step;
import com.mesosphere.sdk.scheduler.plan.strategy.ParallelStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.state.StateStore;

/**
 * Handles creation of the uninstall plan, returning information about the plan contents back to the caller.
 */
class UninstallPlanBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(UninstallPlanBuilder.class);

    private static final String RESOURCE_PHASE = "unreserve-resources";
    private static final String DEREGISTER_PHASE = "deregister-service";
    private static final String TLS_CLEANUP_PHASE = "tls-cleanup";

    private final List<Step> resourceSteps;
    private final Optional<DeregisterStep> deregisterStep;
    private final Plan plan;

    UninstallPlanBuilder(
            String serviceName,
            StateStore stateStore,
            SchedulerFlags schedulerFlags,
            Optional<SecretsClient> secretsClient) {
        // If there is no framework ID, wipe ZK and produce an empty COMPLETE plan
        if (!stateStore.fetchFrameworkId().isPresent()) {
            LOGGER.info("Framework ID is unset. Clearing state data and using an empty completed plan.");
            stateStore.clearAllData();

            // Fill values with stubs, and use an empty COMPLETE plan:
            resourceSteps = Collections.emptyList();
            deregisterStep = Optional.empty();
            plan = new DefaultPlan(Constants.DEPLOY_PLAN_NAME, Collections.emptyList());
            return;
        }

        List<Phase> phases = new ArrayList<>();

        // Given this scenario:
        // - Task 1: resource A, resource B
        // - Task 2: resource A, resource C
        // Create one UninstallStep per unique Resource, including Executor resources.
        // We filter to unique Resource Id's, because Executor level resources are tracked
        // on multiple Tasks. So in this scenario we should have 3 uninstall steps around resources A, B, and C.
        Collection<Protos.TaskInfo> tasks = stateStore.fetchTasks();
        resourceSteps = ResourceUtils.getResourceIds(ResourceUtils.getAllResources(tasks)).stream()
                .map(resourceId -> new UninstallStep(resourceId))
                .collect(Collectors.toList());
        LOGGER.info("Configuring resource cleanup of {} tasks: {}/{} expected resources have been unreserved",
                tasks.size(),
                resourceSteps.stream().filter(step -> step.getStatus() == Status.COMPLETE).count(),
                resourceSteps.size());
        phases.add(new DefaultPhase(RESOURCE_PHASE, resourceSteps, new ParallelStrategy<>(), Collections.emptyList()));

        if (secretsClient.isPresent()) {
            phases.add(new DefaultPhase(
                    TLS_CLEANUP_PHASE,
                    Collections.singletonList(new TLSCleanupStep(
                            Status.PENDING,
                            secretsClient.get(),
                            SecretNameGenerator.getNamespaceFromEnvironment(serviceName, schedulerFlags))),
                    new SerialStrategy<>(),
                    Collections.emptyList()));
        }

        // We don't have access to the SchedulerDriver yet. That will be set via setSchedulerDriver() below.
        deregisterStep = Optional.of(new DeregisterStep(Status.PENDING, stateStore));
        phases.add(new DefaultPhase(
                DEREGISTER_PHASE,
                Collections.singletonList(deregisterStep.get()),
                new SerialStrategy<>(),
                Collections.emptyList()));

        plan = new DefaultPlan(Constants.DEPLOY_PLAN_NAME, phases);
    }

    /**
     * Returns the plan to be used for uninstalling the service.
     */
    Plan getPlan() {
        return plan;
    }

    /**
     * Returns the resource unreservation steps for all tasks in the service. Some steps may be already marked as
     * {@link Status#COMPLETE} when unreservation has already been performed. An empty list may be returned if no known
     * resources need to be unreserved.
     */
    Collection<Step> getResourceSteps() {
        return resourceSteps;
    }

    /**
     * Passes the provided {@link SchedulerDriver} to underlying plan elements.
     */
    void setSchedulerDriver(SchedulerDriver schedulerDriver) {
        if (deregisterStep.isPresent()) {
            deregisterStep.get().setSchedulerDriver(schedulerDriver);
        }
    }
}
