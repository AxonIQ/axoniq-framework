/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark the workflow definition method.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface Workflow {

    String ATTR_ID_PROPERTY = "idProperty";
    String ATTR_ID_PROPERTY_PROVIDER = "idPropertyProvider";
    String ATTR_START_ON = "startOn";
    String ATTR_WORKFLOW_NAME = "workflowName";
    String ATTR_WORKFLOW_NAMESPACE = "workflowNamespace";

    /**
     * Specifies the name of the workflow.
     *
     * @return workflow name, defaults to empty string, meaning that the full-qualified name and method name of the
     * class is used.
     */
    String workflowName() default "";

    /**
     * Specifies the namespace for workflow events.
     *
     * @return namespace for all workflow events, defaults to empty string.
     */
    String workflowNamespace() default "";

    /**
     * Specifies the qualified name of the event to be used as starting trigger.
     *
     * @return qualified name of the event, defaults to empty string.
     */
    String startOn();

    /**
     * Specifies the property of the trigger event defining the workflow id.
     * <p>Either this property or {@link #idPropertyProvider()} must be specified.</p>
     *
     * @return name of the property, defaults to empty string.
     */
    String idProperty() default "";

    /**
     * Specifies the type of id property provider.
     * <p>Either this property or {@link #idProperty()} must be specified.</p>
     *
     * @return id property provider type.
     */
    Class<? extends WorkflowIdProvider> idPropertyProvider() default PayloadPropertyWorkflowIdProvider.class;
}
