package com.mesosphere.sdk.api;

import com.mesosphere.sdk.api.types.PlanInfo;
import com.mesosphere.sdk.scheduler.plan.*;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class PlansResourceTest {
    private static final int alreadyReportedValue = 208;

    @Mock private Plan mockPlan;
    @Mock private Phase mockPhase;
    @Mock private Step mockStep;
    @Mock private PlanScheduler planScheduler;

    private static final UUID stepId = UUID.randomUUID();
    private static final String stepName = "step-name";
    private static final String unknownStepName = "unknown-step";

    private static final UUID phaseId = UUID.randomUUID();
    private static final String phaseName = "phase-name";
    private static final String unknownPhaseName = "unknown-phase";

    private static final UUID planId = UUID.randomUUID();
    private static final String planName = "test-plan";

    private PlansResource resource;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);

        when(mockStep.getId()).thenReturn(stepId);
        when(mockStep.getName()).thenReturn(stepName);

        when(mockPhase.getChildren()).thenReturn(Arrays.asList(mockStep));
        when(mockPhase.getId()).thenReturn(phaseId);
        when(mockPhase.getName()).thenReturn(phaseName);

        when(mockPlan.getChildren()).thenReturn(Arrays.asList(mockPhase));
        when(mockPlan.getId()).thenReturn(planId);
        when(mockPlan.getName()).thenReturn(planName);

        resource = new PlansResource(Arrays.asList(new DefaultPlanManager(mockPlan)));
        verify(mockPlan).interrupt(); // invoked by DefaultPlanManager
    }

    @Test
    public void testListPlans() {
        when(mockPlan.isComplete()).thenReturn(true);
        Response response = resource.listPlans();
        assertEquals(200, response.getStatus());
        assertEquals(String.format("[\"%s\"]", planName), response.getEntity().toString());
    }

    @Test
    public void testFullInfoComplete() {
        when(mockPlan.isComplete()).thenReturn(true);
        Response response = resource.getPlanInfo(planName);
        assertEquals(200, response.getStatus());
        assertTrue(response.getEntity() instanceof PlanInfo);
    }

    @Test
    public void testFullInfoIncomplete() {
        when(mockPlan.isComplete()).thenReturn(false);
        Response response = resource.getPlanInfo(planName);
        assertEquals(202, response.getStatus());
        assertTrue(response.getEntity() instanceof PlanInfo);
    }

    @Test
    public void testFullInfoUnknownName() {
        when(mockPlan.isComplete()).thenReturn(false);
        Response response = resource.getPlanInfo("bad-name");
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
    }

    @Test
    public void testContinue() {
        Response response = resource.continueCommand(planName, null);
        validateCommandResult(response, "continue", mockPlan);
        verify(mockPlan).proceed();

        response = resource.continueCommand(planName, phaseId.toString());
        validateCommandResult(response, "continue", mockPhase);
        verify(mockPhase).proceed();

        response = resource.continueCommand(planName, phaseName);
        validateCommandResult(response, "continue", mockPhase);
        verify(mockPhase, times(2)).proceed();
    }

    @Test
    public void testContinueUnknownId() {
        Response response = resource.continueCommand("bad-name", null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.continueCommand(planName, "bad-name");
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.continueCommand("bad-name", phaseName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
    }

    @Test
    public void testContinueAlreadyInProgress() {
        when(mockPlan.isInProgress()).thenReturn(true);
        when(mockPhase.isInProgress()).thenReturn(true);
        when(mockStep.isInProgress()).thenReturn(true);

        Response response = resource.continueCommand(planName, null);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());

        response = resource.continueCommand(planName, phaseName);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());
    }

    @Test
    public void testContinueAlreadyCompleted() {
        when(mockPlan.isComplete()).thenReturn(true);
        when(mockPhase.isComplete()).thenReturn(true);
        when(mockStep.isComplete()).thenReturn(true);

        Response response = resource.continueCommand(planName, null);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());

        response = resource.continueCommand(planName, phaseName);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());
    }

    @Test
    public void testInterrupt() {
        Response response = resource.interruptCommand(planName, null);
        validateCommandResult(response, "interrupt", mockPlan);
        verify(mockPlan, times(2)).interrupt(); // already called once by DefaultPlanManager constructor

        response = resource.interruptCommand(planName, phaseId.toString());
        validateCommandResult(response, "interrupt", mockPhase);
        verify(mockPhase).interrupt();

        response = resource.interruptCommand(planName, phaseName);
        validateCommandResult(response, "interrupt", mockPhase);
        verify(mockPhase, times(2)).interrupt();
    }

    @Test
    public void testInterruptUnknownId() {
        Response response = resource.interruptCommand("bad-name", null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.interruptCommand(planName, "bad-name");
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.interruptCommand("bad-name", phaseName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
    }

    @Test
    public void testInterruptAlreadyInterrupted() {
        when(mockPlan.isInterrupted()).thenReturn(true);
        when(mockPhase.isInterrupted()).thenReturn(false);

        Response response = resource.interruptCommand(planName, null);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());

        when(mockPlan.isInterrupted()).thenReturn(false);
        when(mockPhase.isInterrupted()).thenReturn(true);
        response = resource.interruptCommand(planName, phaseName);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());
    }

    @Test
    public void testInterruptAlreadyCompleted() {
        when(mockPlan.isComplete()).thenReturn(true);

        Response response = resource.interruptCommand(planName, null);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());

        when(mockPlan.isComplete()).thenReturn(false);
        when(mockPhase.isComplete()).thenReturn(true);
        response = resource.interruptCommand(planName, phaseName);
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());
    }

    @Test
    public void testForceComplete() {
        Response response = resource.forceCompleteCommand(planName, null, null);
        validateCommandResult(response, "forceComplete", mockPlan);
        verify(mockPlan).forceComplete();

        response = resource.forceCompleteCommand(planName, phaseId.toString(), null);
        validateCommandResult(response, "forceComplete", mockPhase);
        verify(mockPhase).forceComplete();

        response = resource.forceCompleteCommand(planName, phaseId.toString(), stepId.toString());
        validateCommandResult(response, "forceComplete", mockStep);
        verify(mockStep).forceComplete();

        response = resource.forceCompleteCommand(planName, null, null);
        validateCommandResult(response, "forceComplete", mockPlan);
        verify(mockPlan, times(2)).forceComplete();

        response = resource.forceCompleteCommand(planName, phaseName, null);
        validateCommandResult(response, "forceComplete", mockPhase);
        verify(mockPhase, times(2)).forceComplete();

        response = resource.forceCompleteCommand(planName, phaseName, stepName);
        validateCommandResult(response, "forceComplete", mockStep);
        verify(mockStep, times(2)).forceComplete();
    }

    @Test
    public void testForceCompleteUnknownId() {
        Response response = resource.forceCompleteCommand("bad-name", phaseName, stepName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.forceCompleteCommand(planName, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.forceCompleteCommand(planName, unknownPhaseName, unknownStepName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);
    }

    @Test
    public void testForceCompleteAlreadyCompleted() {
        when(mockStep.isComplete()).thenReturn(true);

        Response response = resource.forceCompleteCommand(planName, phaseId.toString(), stepId.toString());
        assertEquals(alreadyReportedValue, response.getStatusInfo().getStatusCode());
    }

    @Test
    public void testRestart() {
        Response response = resource.restartCommand(planName, null, null);
        validateCommandResult(response, "restart", mockPlan);
        verify(mockPlan).restart();
        verify(mockPlan).proceed();

        response = resource.restartCommand(planName, phaseId.toString(), null);
        validateCommandResult(response, "restart", mockPhase);
        verify(mockPhase).restart();
        verify(mockPhase).proceed();

        response = resource.restartCommand(planName, phaseId.toString(), stepId.toString());
        validateCommandResult(response, "restart", mockStep);
        verify(mockStep).restart();
        verify(mockStep).proceed();

        response = resource.restartCommand(planName, null, null);
        validateCommandResult(response, "restart", mockPlan);
        verify(mockPlan, times(2)).restart();
        verify(mockPlan, times(2)).proceed();

        response = resource.restartCommand(planName, phaseName, null);
        validateCommandResult(response, "restart", mockPhase);
        verify(mockPhase, times(2)).restart();
        verify(mockPhase, times(2)).proceed();

        response = resource.restartCommand(planName, phaseName, stepName);
        validateCommandResult(response, "restart", mockStep);
        verify(mockStep, times(2)).restart();
        verify(mockStep, times(2)).proceed();
    }

    @Test
    public void testRestartUnknownId() {
        Response response = resource.restartCommand("bad-name", null, null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.restartCommand("bad-name", phaseName, null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.restartCommand("bad-name", phaseName, stepName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.restartCommand(planName, UUID.randomUUID().toString(), null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.restartCommand(planName, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);

        response = resource.restartCommand(planName, unknownPhaseName, unknownStepName);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
        verifyZeroInteractions(mockStep);
    }

    @Test
    public void testStart() {
        when(mockPlan.isComplete()).thenReturn(false);
        Response response = resource.startPlan(planName, Collections.singletonMap("SOME_ENVVAR", "val"));
        assertEquals(Response.Status.OK, response.getStatusInfo());
        verify(mockPlan, times(0)).restart();
        verify(mockPlan).proceed();
    }

    @Test
    public void testStartAlreadyStarted() {
        when(mockPlan.isComplete()).thenReturn(true);
        Response response = resource.startPlan(planName, Collections.emptyMap());
        assertEquals(Response.Status.OK, response.getStatusInfo());
        verify(mockPlan).restart();
        verify(mockPlan).proceed();
    }

    @Test
    public void testStartInvalid() {
        Response response = resource.startPlan("bad-plan", Collections.emptyMap());
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.startPlan(planName, Collections.singletonMap("not-valid-envname", "val"));
        assertEquals(Response.Status.BAD_REQUEST, response.getStatusInfo());
    }

    @Test
    public void testStop() {
        Response response = resource.stopPlan(planName);
        assertEquals(Response.Status.OK, response.getStatusInfo());
        verify(mockPlan, times(2)).interrupt(); // already called once by DefaultPlanManager constructor
        verify(mockPlan).restart();
    }

    @Test
    public void testStopInvalid() {
        Response response = resource.stopPlan("bad-plan");
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
    }

    @Test
    public void testRestartPlan() {
        Response response = resource.restartCommand(planName, null, null);
        assertEquals(Response.Status.OK, response.getStatusInfo());
        verifyZeroInteractions(mockPhase);
        verify(mockPlan).restart();
        verify(mockPlan).proceed();
    }

    @Test
    public void testRestartPhase() {
        Response response = resource.restartCommand(planName, phaseName, null);
        assertEquals(Response.Status.OK, response.getStatusInfo());
        verify(mockPhase).restart();
        verify(mockPhase).proceed();
    }

    @Test
    public void testRestartPhaseInvalid() {
        Response response = resource.restartCommand(planName, "bad-phase", null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());
    }

    @Test
    public void testRestartPlanInvalid() {
        Response response = resource.restartCommand("bad-plan", null, null);
        assertEquals(Response.Status.NOT_FOUND, response.getStatusInfo());

        response = resource.restartCommand(planName, null, "non-null");
        assertEquals(Response.Status.BAD_REQUEST, response.getStatusInfo());
    }

    private static void validateCommandResult(Response response, String commandName, Element... elements) {
        JSONObject expectedObject = new JSONObject();
        expectedObject.put("message", "Received cmd: " + commandName);
        for (int i = 0; i < elements.length; ++i) {
            JSONObject expectedElement = new JSONObject();
            Element element = elements[i];
            expectedElement.put("id", element.getId());
            expectedElement.put("name", element.getName());
            expectedElement.put("type", element.getClass().getSimpleName());
            expectedObject.append("elements", expectedElement);
        }
        assertEquals(expectedObject.toString(2), response.getEntity().toString());
    }
}
