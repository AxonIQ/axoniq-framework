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
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Customizes workflow event names.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface EventNameCustomizer {

    /**
     * Returns a customized event name based on the provided step name, parameters, and step status.
     *
     * @param stepName   the name of the step
     * @param parameters the parameters associated with the step
     * @param stepStatus the status of the step
     * @return the customized event name.
     */
    QualifiedName getEventName(String stepName, Map<String, @Nullable Object> parameters,
                               StepStatus stepStatus);

    /**
     * Returns a customized event name based on the provided step name, parameters, and workflow status.
     *
     * @param stepName   the name of the step
     * @param parameters the parameters associated with the step
     * @param stepStatus the status of the step
     * @return the customized event name.
     */
    QualifiedName getEventName(String stepName, Map<String, @Nullable Object> parameters,
                               WorkflowStatus stepStatus);

    /**
     * Returns a customized event name for a migration step produced by
     * {@code ctx.migrateVersion(changeId, newVersion)}. The {@code changeId} is the semantic anchor; the resulting
     * wire-level name describes <em>what</em> changed (default suffix {@code .Versioned}, e.g.
     * {@code Payment-redesign.Versioned}).
     * <p>
     * The interface default derives a name from {@link #getEventName(String, Map, StepStatus)} with
     * {@link StepStatus#COMPLETED} and appends a {@code .Versioned} marker so a migration event stays distinguishable
     * on the wire from an ordinary completed-step event (which matters in an event-sourced engine — colliding names
     * risk replay drift). {@link DefaultEventNameCustomizer} overrides this with the canonical
     * {@code <changeId>.Versioned} form; custom customizers that do not override it still get a distinct name from this
     * default.
     *
     * @param changeId   developer-chosen identifier of the code change.
     * @param parameters parameters associated with the event.
     * @return the customized event name.
     */
    default QualifiedName versionMigrationEventName(String changeId,
                                                    Map<String, @Nullable Object> parameters) {
        var completed = getEventName(changeId, parameters, StepStatus.COMPLETED);
        return new QualifiedName(completed.namespace(), completed.localName() + ".Versioned");
    }

    /**
     * Returns a customized event name based on the provided step name, parameters, and workflow status.
     *
     * @return event name customizer that will use the namespace of the parent step.
     */
    EventNameCustomizer forStepInheritance();
}
