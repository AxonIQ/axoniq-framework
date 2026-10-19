/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.annotation;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method marker to register a workflow lifecycle completed successfully listener.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@WorkflowStatusChangedHandler(workflowStatus = WorkflowStatus.COMPLETED)
public @interface WorkflowCompletedHandler {

    /**
     * Specifies the name of the workflow.
     *
     * @return workflow name, defaults to empty string, meaning that the full-qualified name and method name of the
     * class are used
     */
    String workflowName() default "";
}
