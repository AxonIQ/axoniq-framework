/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.axonframework.common.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Default implementation of {@link WorkflowTestFixture}.
 *
 * @param <T>      type of the workflow context
 * @param <ACTION> type of the action phase used for {@code given()} and {@code when()}
 * @param <ASSERT> type of the assertion phase used for {@code then()}
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class DefaultWorkflowTestFixture<
        T extends WorkflowContext,
        ACTION extends GivenWhen<ACTION, ASSERT>,
        ASSERT extends Then<ASSERT, ACTION>
        > implements WorkflowTestFixture<ACTION, ASSERT> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ACTION givenWhenPhase;
    private final ASSERT thenPhase;

    /**
     * Creates a fixture instance and binds the supplied phases to a shared workflow test driver.
     *
     * @param workflowModule workflow module to test
     * @param customize      customizer of the workflow configuration
     * @param givenWhenPhase phase instance used for {@code given()} and {@code when()}
     * @param thenPhase      phase instance used for {@code then()}
     */
    DefaultWorkflowTestFixture(
            WorkflowModule<T> workflowModule,
            UnaryOperator<WorkflowConfigurer> customize,
            ACTION givenWhenPhase,
            ASSERT thenPhase
    ) {
        var baseDriver = (DefaultWorkflowTestDriver) WorkflowTestDriver.stepper(
                Objects.requireNonNull(workflowModule, "Workflow module must not be null"),
                true,
                Objects.requireNonNull(customize, "Customizer must not be null"));

        this.givenWhenPhase = Objects.requireNonNull(givenWhenPhase, "Given-when phase must not be null");
        this.thenPhase = Objects.requireNonNull(thenPhase, "Then phase must not be null");

        this.givenWhenPhase.initialize(this, baseDriver);
        this.thenPhase.initialize(this, new DefaultWorkflowTestDriver(baseDriver.workflowTestServices(),
                                                                      baseDriver.mutableTestingState(),
                                                                      AssertionFailureHandler.handler(false),
                                                                      baseDriver.steppingMode()));
    }

    @Override
    public ACTION given() {
        return givenWhenPhase;
    }

    @Override
    public ACTION when() {
        return givenWhenPhase;
    }

    @Override
    public ASSERT then() {
        return thenPhase;
    }
}
