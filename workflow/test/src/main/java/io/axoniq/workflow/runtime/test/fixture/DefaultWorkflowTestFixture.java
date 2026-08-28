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
package io.axoniq.workflow.runtime.test.fixture;

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
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
 * @since 1.0.0
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
