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
