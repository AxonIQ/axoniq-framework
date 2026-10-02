/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.function.UnaryOperator;

/**
 * A workflow starts from an event with a null field, and a step action may return null.
 */
class WorkflowNullPayloadValueTest {

    private WorkflowTestFixture<?, ?> fixture;

    @BeforeEach
    void setUp() {
        var module = WorkflowModule.defaults("NullPayloadValue", SimpleWorkflowContext.class)
                                   .definition(d -> d.autodetected(c -> new NoteWorkflow()));
        fixture = WorkflowTestFixture.of(module, UnaryOperator.identity());
    }

    @AfterEach
    void tearDown() {
        fixture.then().stop();
    }

    @Test
    void nullStartEventFieldAndNullStepResultCompleteTheWorkflow() {
        // when
        fixture.when().publishEvent(new Placed("o-1", null)).execute("readNote");

        // then
        fixture.then().workflowFinished(WorkflowStatus.COMPLETED);
    }

    @Event(namespace = "io.axoniq.framework.workflow.test", name = "Placed")
    record Placed(String orderId, @Nullable String note) {
    }

    public static class NoteWorkflow {

        @Workflow(workflowName = "Note", workflowNamespace = "io.axoniq.framework.workflow.test.nullvalue",
                  idProperty = "orderId", startOnEventClass = Placed.class)
        public void execute(SimpleWorkflowContext ctx) {
            var note = (String) ctx.workflowPayload().get("note");
            ctx.awaitExecute("readNote", String.class, () -> note);
        }
    }
}
