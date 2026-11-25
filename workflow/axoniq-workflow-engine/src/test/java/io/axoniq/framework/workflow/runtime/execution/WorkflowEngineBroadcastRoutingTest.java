/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a business event broadcast to every segment reaches each workflow execution once, through its owning
 * segment, and that the routing work of a segment involves only the executions it owns: every segment of the processor
 * handles a broadcast event, so routing work that grows with the executions of other segments grows quadratically with
 * the executions on the node.
 *
 * @author Allard Buijze
 */
class WorkflowEngineBroadcastRoutingTest {

    private static final List<Segment> SIXTEEN_SEGMENTS = Segment.splitBalanced(Segment.ROOT_SEGMENT, 15);

    private LiveWorkflows liveWorkflows;
    private WorkflowEngine engine;
    private List<InspectionCountingWorkflowExecution> executions;

    @BeforeEach
    void setUp() {
        liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
        engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                    liveWorkflows,
                                    new RecordingWorkflowStore(),
                                    WorkflowIndex.none(),
                                    "WorkflowEngine",
                                    new SimpleUnitOfWorkFactory(new ApplicationContext() {
                                        @Override
                                        public <C> C component(Class<C> type, String name) {
                                            throw new ComponentNotFoundException(type, name);
                                        }
                                    }));
        engine.setCheckpointingSupport(new WorkflowEngineCheckpointingSupport(engine, 0L));
        executions = IntStream.range(0, 2_000)
                              .mapToObj(i -> new InspectionCountingWorkflowExecution("broadcast-" + i))
                              .toList();
        executions.forEach(execution -> liveWorkflows.enter(execution.id(), false, () -> execution));
    }

    private static ProcessingContext contextOf(Segment segment) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        return context;
    }

    @Test
    void aBroadcastEventReachesEveryExecutionOnceThroughItsOwningSegment() {
        // given
        var event = EventTestUtils.asEventMessage("business-event");

        // when
        SIXTEEN_SEGMENTS.forEach(segment -> engine.handle(event, contextOf(segment)));

        // then
        assertThat(executions).allSatisfy(execution -> assertThat(execution.deliveredEvents()).containsExactly(event));
    }

    @Test
    void aSegmentRoutingABroadcastEventInspectsNoExecutionOfAnotherSegment() {
        // given
        var event = EventTestUtils.asEventMessage("business-event");

        for (var segment : SIXTEEN_SEGMENTS) {
            Map<InspectionCountingWorkflowExecution, Integer> inspectionsBefore =
                    executions.stream().collect(Collectors.toMap(Function.identity(),
                                                                 InspectionCountingWorkflowExecution::inspections));

            // when
            engine.handle(event, contextOf(segment));

            // then
            assertThat(executions.stream()
                                 .filter(execution -> !segment.matches(execution.id()))
                                 .filter(execution -> execution.inspections() != inspectionsBefore.get(execution)))
                    .as("executions of other segments than %s inspected while it routed the event", segment)
                    .isEmpty();
        }
    }
}
