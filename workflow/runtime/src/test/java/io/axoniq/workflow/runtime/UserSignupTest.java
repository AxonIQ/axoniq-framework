package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.context.StepFailedException;
import io.axoniq.workflow.runtime.context.WorkflowContext;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UserSignupTest {

    private StateManager stateManager;
    private WorkflowEngine engine;


    @BeforeEach
    void setUp() {
        stateManager = new StateManager();
        engine = new WorkflowEngine(stateManager);
    }

    @AfterEach
    void printEvents() {
        stateManager.printPayloads();
    }

    record User(String id, String email) {
    }

    static class UserSignupWorkflow implements WorkflowDefinition {

        @Override
        public void execute(WorkflowContext context, Map<String, Object> args) {
            User user = (User)args.get("user");

            var success = context.run("createUser", Boolean.class, () -> {
                //
                return true;
            });

            var user2 = args.get("user");

            if (!success) {
                return;
            }



            context.run("activateUser", () -> {
                // var user3 = args.get("user"); // access global workflow payload
                //
                // args.put("user", new User("user-234", "hello@step.biz")); // writing back to global payload bypassing event scope
            });

            context.run("sendWelcomeEmail", () -> {
                //
            });

        }
    }

    @Test
    void shouldExecuteAllStepsOnFirstRun() {
        User user = new User("user-123", "test@example.com");
        String workflowId = "signup-" + user.id();

        engine.execute(workflowId, new UserSignupWorkflow(), Map.of("user", user));
        stateManager.printPayloads();

        assertEquals(Set.of("createUser", "activateUser", "sendWelcomeEmail"), engine.context.steps.keySet());

        // Verify events published
        var events = stateManager.getEventPayloads(workflowId);
        assertEquals(6, events.size()); // 3 starts + 3 completes
        assertInstanceOf(StepStarted.class, events.get(0));
        assertInstanceOf(StepCompleted.class, events.get(1));
        assertInstanceOf(StepStarted.class, events.get(2));
        assertInstanceOf(StepCompleted.class, events.get(3));
        assertInstanceOf(StepStarted.class, events.get(4));
        assertInstanceOf(StepCompleted.class, events.get(5));
    }


    @Test
    void shouldReturnCachedResultForCompletedSteps() {
        String workflowId = "signup001";

        WorkflowDefinition workflow = (context, args) -> {
            String value = context.run("getValue", String.class, () -> "cached-value");
        };

        // First execution
        System.out.println("--- First execution ---");
        engine.execute(workflowId, workflow);

        // Verify events after first run
        var eventsAfterFirst = stateManager.getEventPayloads(workflowId);
        assertEquals(2, eventsAfterFirst.size());
        assertInstanceOf(StepStarted.class, eventsAfterFirst.get(0));
        assertInstanceOf(StepCompleted.class, eventsAfterFirst.get(1));
        assertEquals("cached-value", ((StepCompleted) eventsAfterFirst.get(1)).result());

        // Second execution - should return cached value
        System.out.println("--- Second execution (replay) ---");
        engine.execute(workflowId, workflow);

        // Verify no new events on replay
        var eventsAfterSecond = stateManager.getEventPayloads(workflowId);
        assertEquals(2, eventsAfterSecond.size(), "No new events should be published on replay");
    }

}
