/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;

import java.util.List;

/**
 * Primitive operations and execution metadata used by the workflow runtime.
 *
 * <p>This contract is an internal boundary between the author-facing WorkflowContext and runtime
 * delegates.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface WorkflowExecutionOperations extends
        WorkflowEventPublicationContext,
        ExecutePrimitive,
        WaitForPrimitive,
        WorkflowLifecycleControl,
        PayloadPrimitive,
        VersionPrimitive,
        PublishPrimitive,
        AllMatchCombinator,
        NoneMatchCombinator,
        AnyMatchCombinator,
        DescribableComponent {

    /**
     * Retrieves workflow status.
     *
     * @return the status of the workflow.
     */
    WorkflowStatus workflowStatus();

    /**
     * Retrieves workflow step names.
     *
     * @return the names of the workflow steps.
     */
    List<String> workflowStepNames();
}
