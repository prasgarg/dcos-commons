package com.mesosphere.sdk.api;

import com.mesosphere.sdk.api.types.PlanInfo;
import com.mesosphere.sdk.api.types.PrettyJsonResource;
import com.mesosphere.sdk.offer.evaluate.placement.RegexMatcher;
import com.mesosphere.sdk.offer.evaluate.placement.StringMatcher;
import com.mesosphere.sdk.scheduler.plan.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;

import static com.mesosphere.sdk.api.ResponseUtils.*;

/**
 * API for management of Plan(s).
 */
@Path("/v1")
public class PlansResource extends PrettyJsonResource {

    private static final StringMatcher ENVVAR_MATCHER = RegexMatcher.create("[A-Za-z_][A-Za-z0-9_]*");
    private static final Logger LOGGER = LoggerFactory.getLogger(PlansResource.class);

    private final Collection<PlanManager> planManagers;

    public PlansResource(Collection<PlanManager> planManagers) {
        this.planManagers = planManagers;
    }

    /**
     * Returns list of all configured plans.
     */
    @GET
    @Path("/plans")
    public Response listPlans() {
        return jsonOkResponse(new JSONArray(getPlanNames()));
    }

    /**
     * Returns a full list of the {@link Plan}'s contents (incl all {@link Phase}s/{@link Step}s).
     */
    @GET
    @Path("/plans/{planName}")
    public Response getPlanInfo(@PathParam("planName") String planName) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Plan plan = planManagerOptional.get().getPlan();
        return jsonResponseBean(
                PlanInfo.forPlan(plan),
                plan.isComplete() ? Response.Status.OK : Response.Status.ACCEPTED);
    }

    /**
     * Idempotently starts a plan.  If a plan is complete, it restarts the plan.  If it is interrupted, in makes the
     * plan proceed.  If a plan is already in progress, it has no effect.
     */
    @POST
    @Path("/plans/{planName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response startPlan(@PathParam("planName") String planName, Map<String, String> parameters) {
        try {
            validate(parameters);
        } catch (ValidationException e) {
            return invalidParameterResponse(e.getMessage());
        }

        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Plan plan = planManagerOptional.get().getPlan();
        plan.updateParameters(parameters);
        if (plan.isComplete()) {
            plan.restart();
        }
        plan.proceed();
        return jsonOkResponse(getCommandResult("start", Arrays.asList(plan)));
    }

    /**
     * Idempotently stops a plan.  If a plan is in progress, it is interrupted and the plan is reset such that all
     * elements are waiting/interrupted.  If a plan is already stopped, it has no effect.
     */
    @POST
    @Path("/plans/{planName}/stop")
    public Response stopPlan(@PathParam("planName") String planName) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Plan plan = planManagerOptional.get().getPlan();

        boolean interruptChanged = plan.interrupt();
        boolean restartChanged = !plan.restart().isEmpty();
        return jsonOkResponse(getCommandResult("stop",
                interruptChanged || restartChanged ? Collections.singleton(plan) : Collections.emptyList()));
    }

    @POST
    @Path("/plans/{planName}/continue")
    public Response continueCommand(
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Optional<Element> elementOptional =
                getPlanElement(planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.empty());
        if (!elementOptional.isPresent()) {
            return phaseNotFoundResponse(planName, phase);
        }
        Element element = elementOptional.get();

        if (element.isComplete() || element.isInProgress()) {
            return noChangeResponse(element);
        }

        return jsonOkResponse(getCommandResult("continue",
                element.proceed() ? Collections.singleton(element) : Collections.emptyList()));
    }

    @POST
    @Path("/plans/{planName}/interrupt")
    public Response interruptCommand(
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Optional<Element> elementOptional =
                getPlanElement(planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.empty());
        if (!elementOptional.isPresent()) {
            return phaseNotFoundResponse(planName, phase);
        }
        Element element = elementOptional.get();

        if (element.isComplete() || element.isInterrupted()) {
            return noChangeResponse(element);
        }

        return jsonOkResponse(getCommandResult("interrupt",
                element.interrupt() ? Collections.singleton(element) : Collections.emptyList()));
    }

    @POST
    @Path("/plans/{planName}/forceComplete")
    public Response forceCompleteCommand(
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase,
            @QueryParam("step") String step) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        if (phase == null && step != null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        Optional<Element> elementOptional = getPlanElement(
                planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.ofNullable(step));
        if (!elementOptional.isPresent()) {
            return phaseOrStepNotFoundResponse(planName, phase, step);
        }
        Element element = elementOptional.get();

        if (element.isComplete()) {
            return noChangeResponse(element);
        }

        return jsonOkResponse(getCommandResult("forceComplete", element.forceComplete()));
    }

    @POST
    @Path("/plans/{planName}/restart")
    public Response restartCommand(
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase,
            @QueryParam("step") String step) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        if (phase == null && step != null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        Optional<Element> elementOptional = getPlanElement(
                planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.ofNullable(step));
        if (!elementOptional.isPresent()) {
            return phaseOrStepNotFoundResponse(planName, phase, step);
        }
        Element element = elementOptional.get();

        Collection<? extends Element> modifiedElements = element.restart(); // set element status(es) to pending
        element.proceed(); // also clear interrupted bit on element, if set
        return jsonOkResponse(getCommandResult("restart", modifiedElements));
    }

    @GET
    @Deprecated
    @Path("/plan")
    public Response getFullInfo() {
        return getPlanInfo("deploy");
    }

    @POST
    @Deprecated
    @Path("/plan/continue")
    public Response continueCommand() {
        return continueCommand("deploy", null);
    }

    @POST
    @Deprecated
    @Path("/plan/interrupt")
    public Response interruptCommand() {
        return interruptCommand("deploy", null);
    }

    @POST
    @Deprecated
    @Path("/plan/forceComplete")
    public Response forceCompleteCommand(
            @QueryParam("phase") String phaseId,
            @QueryParam("step") String stepId) {
        return forceCompleteCommand("deploy", phaseId, stepId);
    }

    @POST
    @Deprecated
    @Path("/plan/restart")
    public Response restartCommand(
            @QueryParam("phase") String phaseId,
            @QueryParam("step") String stepId) {
        return restartCommand("deploy", phaseId, stepId);
    }

    /**
     * Returns the matching plan, phase(s), or step for the provided filters.
     * Returns an empty list if no match was found.
     *
     * <ul>
     * <li>If neither the phase nor the step is specified: Returns the plan</li>
     * <li>If only the phase is specified: Returns matching phase(s), if any</li>
     * <li>If the phase and the step are both specified: Returns matching step, if any</li>
     * </ul>
     */
    private static Optional<Element> getPlanElement(
            Plan plan, Optional<String> phaseIdOrName, Optional<String> stepIdOrName) {
        if (phaseIdOrName.isPresent()) {
            Optional<Phase> phase = getPhase(plan, phaseIdOrName.get());
            if (!phase.isPresent()) {
                return Optional.empty();
            }
            if (stepIdOrName.isPresent()) {
                // Find step, convert Optional<Step> to Optional<Element>
                return convert(getStep(phase.get(), stepIdOrName.get()));
            }
            // Phase found, convert Optional<Phase> to Optional<Element>
            return convert(getStep(phase.get(), stepIdOrName.get()));
        } else {
            if (stepIdOrName.isPresent()) {
                // Caller should have checked this, but just in case..
                throw new IllegalStateException(String.format(
                        "Phase must be provided when step (%s) is provided", stepIdOrName.get()));
            }
            // Just return plan
            return Optional.of(plan);
        }
    }

    private static Optional<Element> convert(Optional<? extends Element> o) {
        return Optional.ofNullable(o.orElse(null));
    }

    /**
     * Returns the phase which matches the provided filter, or an empty {@link Optional} if none/multiple match.
     *
     * @param phaseIdOrName a valid UUID or name of the phases to search for in the {@code plan}
     */
    private static Optional<Phase> getPhase(Plan plan, String phaseIdOrName) {
        List<Phase> phases;
        try {
            UUID phaseId = UUID.fromString(phaseIdOrName);
            phases = plan.getChildren().stream()
                    .filter(phase -> phase.getId().equals(phaseId))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            // couldn't parse as UUID: fall back to treating phase identifier as a name
            phases = plan.getChildren().stream()
                    .filter(phase -> phase.getName().equals(phaseIdOrName))
                    .collect(Collectors.toList());
        }
        if (phases.size() == 1) {
            return Optional.of(phases.get(0));
        } else {
            LOGGER.warn("Expected 1 phase '{}' in plan {}, got: {}", phaseIdOrName, plan.getName(), phases);
            return Optional.empty();
        }
    }

    /**
     * Returns the step which matches the provided filter, or an empty {@link Optional} if none/multiple match.
     *
     * @param stepIdOrName a valid UUID or name of the step to search for in the {@code phases}
     */
    private static Optional<Step> getStep(Phase phase, String stepIdOrName) {
        List<Step> steps;
        try {
            UUID stepId = UUID.fromString(stepIdOrName);
            steps = phase.getChildren().stream()
                    .filter(step -> step.getId().equals(stepId))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            // couldn't parse as UUID: fall back to treating step identifier as a name
            steps = phase.getChildren().stream()
                    .filter(step -> step.getName().equals(stepIdOrName))
                    .collect(Collectors.toList());
        }
        if (steps.size() == 1) {
            return Optional.of(steps.get(0));
        } else {
            LOGGER.warn("Expected 1 step '{}' in phase {}, got: {}", stepIdOrName, phase.getName(), steps);
            return Optional.empty();
        }
    }

    private List<String> getPlanNames() {
        return planManagers.stream()
                .map(planManager -> planManager.getPlan().getName())
                .collect(Collectors.toList());
    }

    private Optional<PlanManager> getPlanManager(String planName) {
        return planManagers.stream()
                .filter(planManager -> planManager.getPlan().getName().equals(planName))
                .findFirst();
    }

    private static Response invalidParameterResponse(String message) {
        return plainResponse(
                String.format("Couldn't parse parameters: %s", message),
                Response.Status.BAD_REQUEST);
    }

    private static void validate(Map<String, String> parameters) throws ValidationException {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            if (!ENVVAR_MATCHER.matches(entry.getKey())) {
                throw new ValidationException(
                        String.format("%s is not a valid environment variable name", entry.getKey()));
            }
        }
    }

    private static class ValidationException extends Exception {
        public ValidationException(String message) {
            super(message);
        }
    }

    private static JSONObject getCommandResult(String command, Collection<? extends Element> elements) {
        JSONObject result = new JSONObject();
        result.put("message", String.format("Received cmd: %s", command));
        for (Element elem : elements) {
            JSONObject elemJson = new JSONObject();
            elemJson.put("id", elem.getId());
            elemJson.put("name", elem.getName());
            elemJson.put("type", elem.getClass().getSimpleName());
            result.append("elements", elemJson);
        }
        return result;
    }

    /**
     * Returns a 404 Not found response for the given plan.
     */
    private static Response planNotFoundResponse(String plan) {
        return plainResponse(String.format("Plan '%s' not found", plan), Response.Status.NOT_FOUND);
    }

    /**
     * Returns a 404 Not found response for the given phase.
     */
    private static Response phaseNotFoundResponse(String plan, String phase) {
        return plainResponse(
                String.format("Phase '%s' not found in plan '%s'", phase, plan),
                Response.Status.NOT_FOUND);
    }

    /**
     * Returns a 404 Not found response for the given phase or step.
     */
    private static Response phaseOrStepNotFoundResponse(String plan, String phase, String step) {
        return plainResponse(
                String.format("Phase '%s' or step '%s' not found in plan '%s'. "
                        + "If multiple elements with that name exist, you must use the UUID", phase, step, plan),
                Response.Status.NOT_FOUND);
    }

    /**
     * Returns a "208 Already reported" response.
     */
    private static Response noChangeResponse(Element element) {
        return plainResponse(String.format(
                "Element '%s' is already in state %s, nothing to do", element.getName(), element.getStatus()), 208);
    }
}
