/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.annotation;

import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark the workflow definition method.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface Workflow {

    String ATTR_ID_PROPERTY = "idProperty";
    String ATTR_ID_PROPERTY_PROVIDER = "idPropertyProvider";
    String ATTR_START_ON_EVENT_NAME = "startOnEventName";
    String ATTR_START_ON_EVENT_CLASS = "startOnEventClass";
    String ATTR_START_ON_CONDITIONS = "startOnConditions";
    String ATTR_WORKFLOW_NAME = "workflowName";
    String ATTR_WORKFLOW_STATUS = "workflowStatus";
    String ATTR_WORKFLOW_NAMESPACE = "workflowNamespace";
    String ATTR_WORKFLOW_VERSION = "workflowVersion";

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
     * Specifies the qualified name of the event to be used as a starting trigger. If this property is set, the
     * {@link #startOnEventClass()} property must not be set.
     *
     * @return qualified name of the event, defaults to empty string.
     */
    String startOnEventName() default "";

    /**
     * Specifies the class of the event to be used as a starting trigger. If this property is set, the
     * {@link #startOnEventName()} property must not be set.
     *
     * @return class of the event, defaults to Void.class.
     */
    Class<?> startOnEventClass() default Void.class;

    /**
     * List of start conditions expressed as associations.
     *
     * @return array of value associations.
     */
    String[] startOnConditions() default {};

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

    /**
     * Workflow definition version (semver, e.g. {@code "0.0.2"}). New instances start on the highest registered
     * version; in-flight instances replay on the version they were started under.
     * <p>
     * Defaults to {@link Version#DEFAULT_VERSION} ({@code "0.0.1"}).
     *
     * @return workflow definition version.
     */
    String workflowVersion() default Version.DEFAULT_VERSION;
}
