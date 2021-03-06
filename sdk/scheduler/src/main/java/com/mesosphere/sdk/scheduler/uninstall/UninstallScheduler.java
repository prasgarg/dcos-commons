package com.mesosphere.sdk.scheduler.uninstall;

import com.mesosphere.sdk.api.PlansResource;
import com.mesosphere.sdk.dcos.SecretsClient;
import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.offer.evaluate.security.SecretNameGenerator;
import com.mesosphere.sdk.scheduler.*;
import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.scheduler.plan.strategy.ParallelStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.scheduler.recovery.DefaultTaskFailureListener;
import com.mesosphere.sdk.scheduler.recovery.FailureUtils;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.mesosphere.sdk.offer.Constants.TOMBSTONE_MARKER;

/**
 * This scheduler uninstalls the framework and releases all of its resources.
 */
public class UninstallScheduler extends AbstractScheduler {

    private static final String TASK_KILL_PHASE = "task-kill-phase";
    private static final String RESOURCE_PHASE = "resource-phase";
    private static final String DEREGISTER_PHASE = "deregister-phase";
    private static final String TLS_CLEANUP_PHASE = "tls-cleanup-phase";
    private static final Logger LOGGER = LoggerFactory.getLogger(UninstallScheduler.class);
    protected final int port;
    protected final Optional<SecretsClient> secretsClient;
    protected final String serviceName;
    private final Plan uninstallPlan;
    private final ConfigStore<ServiceSpec> configStore;
    private final SchedulerFlags schedulerFlags;
    PlanManager uninstallPlanManager;
    private TaskKiller taskKiller;
    private OfferAccepter offerAccepter;
    private SchedulerApiServer schedulerApiServer;

    /**
     * Creates a new UninstallScheduler based on the provided API port and initialization timeout,
     * and a {@link StateStore}. The UninstallScheduler builds an uninstall {@link Plan} with two {@link Phase}s:
     * a resource phase where all reserved resources get released back to Mesos, and a deregister phase where
     * the framework deregisters itself and cleans up its state in Zookeeper.
     */
    public UninstallScheduler(
            String serviceName,
            int port,
            Duration apiServerInitTimeout,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            SchedulerFlags schedulerFlags,
            Optional<SecretsClient> secretsClient) {
        super(stateStore);
        this.port = port;
        this.configStore = configStore;
        this.schedulerFlags = schedulerFlags;
        this.secretsClient = secretsClient;
        this.serviceName = serviceName;
        this.uninstallPlan = getPlan();
        this.uninstallPlanManager = new DefaultPlanManager(uninstallPlan);
        LOGGER.info("Initializing plans resource...");
        PlansResource plansResource = new PlansResource(Collections.singletonList(uninstallPlanManager));
        Collection<Object> apiResources = Collections.singletonList(plansResource);
        schedulerApiServer = new SchedulerApiServer(port, apiResources, apiServerInitTimeout);
        new Thread(schedulerApiServer).start();
    }

    public UninstallScheduler(
            String serviceName,
            int port,
            Duration apiServerInitTimeout,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            SchedulerFlags schedulerFlags) {
        this(serviceName, port, apiServerInitTimeout, stateStore, configStore, schedulerFlags, Optional.empty());
    }

    private Plan getPlan() {
        // If there is no framework ID, wipe ZK and return a COMPLETE plan
        if (!stateStore.fetchFrameworkId().isPresent()) {
            LOGGER.info("There is no framework ID so clear service in StateStore and return a COMPLETE plan");
            stateStore.clearAllData();
            return new DefaultPlan(Constants.DEPLOY_PLAN_NAME, Collections.emptyList());
        }

        List<Phase> phases = new ArrayList<>();

        // First, we kill all the tasks, so that we may release their reserved resources.
        List<Step> taskKillSteps = stateStore.fetchTasks().stream()
                .map(Protos.TaskInfo::getTaskId)
                .map(taskID -> new TaskKillStep(taskID))
                .collect(Collectors.toList());
        Phase taskKillPhase = new DefaultPhase(TASK_KILL_PHASE, taskKillSteps, new ParallelStrategy<>(),
                Collections.emptyList());
        phases.add(taskKillPhase);

        // Given this scenario:
        // - Task 1: resource A, resource B
        // - Task 2: resource A, resource C
        // Create one UninstallStep per unique Resource, including Executor resources.
        // We filter to unique Resource Id's, because Executor level resources are tracked
        // on multiple Tasks. So in this scenario we should have 3 uninstall steps around resources A, B, and C.

        // Filter the tasks to those that have actually created resources.
        // If a task has failed its initial launch, it will have a status of TASK_ERROR
        // and its TaskInfo will labeled as permanently failed.
        List<Protos.TaskID> tasksInErrorState = stateStore.fetchStatuses()
                .stream()
                .filter(taskStatus -> taskStatus.getState() == Protos.TaskState.TASK_ERROR)
                .map(Protos.TaskStatus::getTaskId)
                .collect(Collectors.toList());

        List<Protos.TaskInfo> tasksNotFailedAndErrored = stateStore.fetchTasks()
                .stream()
                .filter(taskInfo -> !(FailureUtils.isPermanentlyFailed(taskInfo)
                        && tasksInErrorState.contains(taskInfo.getTaskId())))
                .collect(Collectors.toList());

        List<Protos.Resource> allResources = ResourceUtils.getAllResources(tasksNotFailedAndErrored);
        List<Step> resourceSteps = ResourceUtils.getResourceIds(allResources).stream()
                .map(resourceId -> new ResourceCleanupStep(resourceId, resourceId.startsWith(TOMBSTONE_MARKER) ?
                        Status.COMPLETE : Status.PENDING))
                .collect(Collectors.toList());

        Phase resourcePhase = new DefaultPhase(RESOURCE_PHASE, resourceSteps, new ParallelStrategy<>(),
                Collections.emptyList());
        phases.add(resourcePhase);

        if (secretsClient.isPresent()) {
            Step tlsCleanupStep = new TLSCleanupStep(
                    Status.PENDING,
                    secretsClient.get(),
                    SecretNameGenerator.getNamespaceFromEnvironment(serviceName, schedulerFlags));
            List<Step> tlsCleanupSteps = Collections.singletonList(tlsCleanupStep);
            Phase tlsCleanupPhase = new DefaultPhase(TLS_CLEANUP_PHASE, tlsCleanupSteps, new SerialStrategy<>(),
                    Collections.emptyList());
            phases.add(tlsCleanupPhase);
        }

        // We don't have access to the SchedulerDriver yet, so that gets set later
        Step deregisterStep = new DeregisterStep(stateStore);
        List<Step> deregisterSteps = Collections.singletonList(deregisterStep);
        Phase deregisterPhase = new DefaultPhase(DEREGISTER_PHASE, deregisterSteps, new SerialStrategy<>(),
                Collections.emptyList());
        phases.add(deregisterPhase);

        return new DefaultPlan(Constants.DEPLOY_PLAN_NAME, phases);
    }

    @Override
    protected void initialize(SchedulerDriver driver) throws InterruptedException {
        LOGGER.info("Initializing...");
        // NOTE: We wait until this point to perform any work using configStore/stateStore.
        // We specifically avoid writing any data to ZK before registered() has been called.
        initializeGlobals(driver);
        LOGGER.info("Proceeding with uninstall plan...");
        uninstallPlanManager.getPlan().proceed();
        LOGGER.info("Done initializing.");
    }

    private void initializeGlobals(SchedulerDriver driver) {
        LOGGER.info("Initializing globals...");
        taskKiller = new DefaultTaskKiller(new DefaultTaskFailureListener(stateStore, configStore), driver);
        // Add the task killer to the task kill steps.
        uninstallPlan.getChildren().stream()
                .filter(phase -> phase.getName().equals(TASK_KILL_PHASE))
                .findFirst()
                .get()
                .getChildren()
                .forEach(step -> ((TaskKillStep) step).setTaskKiller(taskKiller));

        // Attach the resource phase to the uninstall recorder.
        Phase resourcePhase = uninstallPlan.getChildren().stream()
                .filter(phase -> phase.getName().equals(RESOURCE_PHASE))
                .findFirst()
                .get();
        UninstallRecorder uninstallRecorder = new UninstallRecorder(stateStore, resourcePhase);
        offerAccepter = new OfferAccepter(Collections.singletonList(uninstallRecorder));

        // Set the driver on the DeregisterStep
        ((DeregisterStep) uninstallPlan.getChildren()
                .stream()
                .filter(phase -> phase.getName().equals(DEREGISTER_PHASE))
                .findFirst()
                .get()
                .getChildren()
                .get(0)).setSchedulerDriver(driver);
    }

    public boolean apiServerReady() {
        return schedulerApiServer.ready();
    }

    @Override
    protected void processOfferSet(List<Protos.Offer> offers) {
        List<Protos.Offer> localOffers = new ArrayList<>(offers);
        // Get candidate steps to be scheduled
        Collection<? extends Step> candidateSteps = uninstallPlanManager.getCandidates(Collections.emptyList());
        if (!candidateSteps.isEmpty()) {
            LOGGER.info("Attempting to process these candidates from uninstall plan: {}",
                    candidateSteps.stream().map(Element::getName).collect(Collectors.toList()));
            candidateSteps.forEach(Step::start);
        }

        // Destroy/Unreserve any reserved resource or volume that is offered
        final List<Protos.OfferID> offersWithReservedResources = new ArrayList<>();

        offersWithReservedResources.addAll(
                new ResourceCleanerScheduler(new UninstallResourceCleaner(), offerAccepter)
                        .resourceOffers(driver, localOffers));

        List<Protos.Offer> unusedOffers = OfferUtils.filterOutAcceptedOffers(
                localOffers,
                offersWithReservedResources);

        // Decline remaining offers.
        OfferUtils.declineOffers(driver, unusedOffers);
    }

    @Override
    protected Collection<PlanManager> getPlanManagers() {
        return Arrays.asList(uninstallPlanManager);
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Received status update for taskId={} state={} message='{}'",
                status.getTaskId().getValue(),
                status.getState().toString(),
                status.getMessage());

        eventBus.post(status);

        try {
            stateStore.storeStatus(status);
            reconciler.update(status);
        } catch (Exception e) {
            LOGGER.warn("Failed to update TaskStatus received from Mesos. "
                    + "This may be expected if Mesos sent stale status information: " + status, e);
        }
    }
}
