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
     * elements are pending.  If a plan is already stopped, it has no effect.
     */
    @POST
    @Path("/plans/{planName}/stop")
    public Response stopPlan(@PathParam("planName") String planName) {
        final Optional<PlanManager> planManagerOptional = getPlanManager(planName);
        if (!planManagerOptional.isPresent()) {
            return planNotFoundResponse(planName);
        }

        Plan plan = planManagerOptional.get().getPlan();
        plan.interrupt();
        plan.restart();
        return jsonOkResponse(getCommandResult("stop", Arrays.asList(plan)));
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

        List<Element> elements =
                getPlanElements(planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.empty());
        if (elements.isEmpty()) {
            return phaseNotFoundResponse(planName, phase);
        }

        List<Element> elementsToContinue = elements.stream()
                .filter(elem -> !elem.isInProgress() && !elem.isComplete())
                .collect(Collectors.toList());
        if (elementsToContinue.isEmpty()) {
            return alreadyReportedResponse();
        }

        elementsToContinue.forEach(Element::proceed);

        return jsonOkResponse(getCommandResult("continue", elementsToContinue));
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

        List<Element> elements =
                getPlanElements(planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.empty());
        if (elements.isEmpty()) {
            return phaseNotFoundResponse(planName, phase);
        }

        List<Element> elementsToInterrupt = elements.stream()
                .filter(elem -> !elem.isComplete() && !elem.isInterrupted())
                .collect(Collectors.toList());
        if (elementsToInterrupt.isEmpty()) {
            return alreadyReportedResponse();
        }

        elementsToInterrupt.forEach(Element::interrupt);

        return jsonOkResponse(getCommandResult("interrupt", elementsToInterrupt));
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

        List<Element> elements = getPlanElements(
                planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.ofNullable(step));
        if (elements.isEmpty()) {
            return phaseOrStepNotFoundResponse(planName, phase, step);
        }

        List<Element> elementsToComplete = elements.stream()
                .filter(elem -> !elem.isComplete())
                .collect(Collectors.toList());
        if (elementsToComplete.isEmpty()) {
            return alreadyReportedResponse();
        }

        elementsToComplete.forEach(Element::forceComplete);

        return jsonOkResponse(getCommandResult("forceComplete", elementsToComplete));
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

        List<Element> elements = getPlanElements(
                planManagerOptional.get().getPlan(), Optional.ofNullable(phase), Optional.ofNullable(step));
        if (elements.isEmpty()) {
            return phaseOrStepNotFoundResponse(planName, phase, step);
        }

        elements.forEach(Element::restart);
        elements.forEach(Element::proceed);

        return jsonOkResponse(getCommandResult("restart", elements));
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
    private static List<Element> getPlanElements(
            Plan plan, Optional<String> phaseIdOrName, Optional<String> stepIdOrName) {
        if (phaseIdOrName.isPresent()) {
            List<Phase> phases = getPhases(plan, phaseIdOrName.get());
            if (stepIdOrName.isPresent()) {
                // Return step, if found
                Optional<Step> step = getStep(phases, stepIdOrName.get());
                return step.isPresent() ? Arrays.asList(step.get()) : Collections.emptyList();
            }
            // Return matching phase(s), with explicit generic type conversion for a happy compiler
            List<Element> elements = new ArrayList<>();
            elements.addAll(phases);
            return elements;
        } else {
            if (stepIdOrName.isPresent()) {
                throw new IllegalStateException(String.format(
                        "Phase must be provided when step (%s) is provided", stepIdOrName.get()));
            }
            // Return plan
            return Arrays.asList(plan);
        }
    }

    /**
     * Returns the phases which match the provided filter, or an empty list if none match.
     *
     * @param phaseIdOrName a valid UUID or name of the phases to search for in the {@code plan}
     */
    private static List<Phase> getPhases(Plan plan, String phaseIdOrName) {
        try {
            UUID phaseId = UUID.fromString(phaseIdOrName);
            return plan.getChildren().stream()
                    .filter(phase -> phase.getId().equals(phaseId))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            // couldn't parse as UUID: fall back to treating phase identifier as a name
            return plan.getChildren().stream()
                    .filter(phase -> phase.getName().equals(phaseIdOrName))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Returns the step which match the provided filter, or an empty {@link Optional} if none match or multiple match.
     *
     * @param stepIdOrName a valid UUID or name of the step to search for in the {@code phases}
     */
    private static Optional<Step> getStep(List<Phase> phases, String stepIdOrName) {
        List<Step> steps;
        try {
            UUID stepId = UUID.fromString(stepIdOrName);
            steps = phases.stream().map(ParentElement::getChildren)
                    .flatMap(List::stream)
                    .filter(step -> step.getId().equals(stepId))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            // couldn't parse as UUID: fall back to treating step identifier as a name
            steps = phases.stream().map(ParentElement::getChildren)
                    .flatMap(List::stream)
                    .filter(step -> step.getName().equals(stepIdOrName))
                    .collect(Collectors.toList());
        }
        if (steps.size() == 1) {
            return Optional.of(steps.get(0));
        } else {
            LOGGER.error("Expected 1 step '{}' across {} phases, got: {}", stepIdOrName, phases.size(), steps);
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

    private static JSONObject getCommandResult(String command, List<Element> elements) {
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
                String.format("Phase '%s' or step '%s' not found in plan '%s'", phase, step, plan),
                Response.Status.NOT_FOUND);
    }

    /**
     * Returns a "208 Already reported" response.
     */
    private static Response alreadyReportedResponse() {
        return plainResponse("Elements are already in requested state, nothing to do", 208);
    }
}
