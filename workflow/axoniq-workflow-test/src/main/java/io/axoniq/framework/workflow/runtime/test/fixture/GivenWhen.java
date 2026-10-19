/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.fixture;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * A stage for defining the workflow actions
 *
 * @param <SELF>   type of action stage
 * @param <ASSERT> type of assert stage
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class GivenWhen<SELF extends GivenWhen<SELF, ASSERT>, ASSERT extends Then<ASSERT, SELF>>
        extends FixturePhase<SELF> {

    /**
     * Default implementation of the {@link GivenWhen}.
     */
    public static final class Phase extends GivenWhen<GivenWhen.Phase, Then.Phase> {

    }

    /**
     * Returns the action phase to use.
     *
     * @return fixture to continue fluent invocations
     */
    public SELF when() {
        return self();
    }

    /**
     * Returns the assert phase to use
     *
     * @return assert phase to continue fluent invocations
     */
    public ASSERT then() {
        //noinspection unchecked
        return (ASSERT) fixture.then();
    }


    /**
     * Publishes an event through the fixture event bus.
     *
     * @param event event payload or an already constructed event message
     * @return this phase for fluent chaining
     */
    public SELF publishEvent(Object event) {
        Objects.requireNonNull(event, "Event must not be null");
        testDriver.publishEvent(event);
        return self();
    }

    /**
     * Releases an execute step by invoking the original workflow-defined action.
     *
     * @param stepName step name to release
     * @return this phase for fluent chaining
     */
    public SELF execute(String stepName) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        testDriver.executeStep(stepName);
        return self();
    }

    /**
     * Supplies a fake action for an execute step and waits until the step reaches a terminal state.
     *
     * @param stepName         step name to release
     * @param payloadProcessor fake action to invoke instead of the workflow-defined action
     * @return this phase for fluent chaining
     */
    public SELF execute(String stepName, PayloadProcessor payloadProcessor) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(payloadProcessor, "Payload processor must not be null");

        testDriver.executeStep(stepName, payloadProcessor);
        return self();
    }

    /**
     * Supplies a fake successful execute-step result and asserts that the step completes.
     *
     * @param stepName        step name to release
     * @param expectedPayload payload returned by the fake step action
     * @return this phase for fluent chaining
     */
    public SELF executeReturning(String stepName, Map<String, @Nullable Object> expectedPayload) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(expectedPayload, "Expected payload must not be null");
        execute(stepName, (pc, in) -> expectedPayload);
        testDriver.testingState().step(
                testDriver.assertionFailureHandler(),
                s -> s.stepName().equals(stepName) && s.status() == StepStatus.COMPLETED
        );
        return self();
    }

    /**
     * Supplies a fake failing execute-step action and asserts that the step fails.
     *
     * @param stepName        step name to release
     * @param expectedFailure failure thrown by the fake action
     * @return this phase for fluent chaining
     */
    public SELF executeFailing(String stepName, StepFailedException expectedFailure) {
        Objects.requireNonNull(stepName, "Step name must not be null");
        Objects.requireNonNull(expectedFailure, "Expected exception must not be null");
        execute(stepName, (pc, in) -> {
            throw expectedFailure;
        });
        testDriver.testingState().step(
                testDriver.assertionFailureHandler(),
                s -> s.stepName().equals(stepName) && s.status() == StepStatus.FAILED
        );
        return self();
    }

    /**
     * Advances fixture-controlled time and runs all due timeout tasks.
     *
     * @param duration duration by which fixture time advances
     * @return this phase for fluent chaining
     */
    public SELF timePasses(Duration duration) {
        Objects.requireNonNull(duration, "Duration must not be null");
        testDriver.timePasses(duration);
        return self();
    }
}
