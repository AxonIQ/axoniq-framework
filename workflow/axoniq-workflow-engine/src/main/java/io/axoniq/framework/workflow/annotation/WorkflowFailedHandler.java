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
package io.axoniq.framework.workflow.annotation;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method marker to register a workflow lifecycle failure listener.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@WorkflowStatusChangedHandler(workflowStatus = WorkflowStatus.FAILED)
public @interface WorkflowFailedHandler {

    /**
     * Specifies the name of the workflow.
     *
     * @return workflow name, defaults to empty string, meaning that the full-qualified name and method name of the
     * class are used
     */
    String workflowName() default "";
}
