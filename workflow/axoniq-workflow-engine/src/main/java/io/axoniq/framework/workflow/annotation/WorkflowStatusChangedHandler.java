/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
 * Method marker to register a workflow lifecycle listener based on the status of the workflow.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface WorkflowStatusChangedHandler {

    /**
     * Specifies the name of the workflow.
     *
     * @return workflow name, defaults to empty string, meaning that the full-qualified name and method name of the
     * class is used
     */
    String workflowName() default "";

    /**
     * Specifies the workflow status to listen for.
     *
     * @return workflow status to listen for
     */
    WorkflowStatus workflowStatus();
}
