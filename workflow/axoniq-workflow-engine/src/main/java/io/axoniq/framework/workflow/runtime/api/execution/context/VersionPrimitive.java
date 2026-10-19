/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;

import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;

/**
 * Primitive for forking workflow logic mid-flight. Behaves like get-or-set on a per-{@code stepName} slot: a fresh live
 * invocation records a migration step and returns the {@code newVersion}; a replay (or a workflow that already executed
 * past this point under old code) returns the version recorded at the time. For larger changes prefer a separate
 * workflow definition with a bumped {@code @Workflow(workflowVersion=...)}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface VersionPrimitive {

    /**
     * Records (or reads back) the migration step for the given {@code stepName}. On the first live invocation with a
     * strictly greater {@code newVersion} the step is appended; on replay (or past the call) the previously recorded
     * value is returned via the result handle.
     *
     * @param command parameter object carrying the {@code stepName} and the desired {@code newVersion}.
     * @return a {@link WorkflowStepResult} that resolves to the version this workflow has committed to for the given
     * {@code stepName}.
     */
    WorkflowStepResult version(VersionCommand command);

    /**
     * Parameter object for the primitive.
     */
    interface VersionCommand {

        /**
         * Logical step name (a developer-chosen identifier describing the change). Forms the wire-level event name of
         * the migration step (e.g. {@code "payment-redesign"} surfaces as {@code Payment-redesign.Versioned}).
         *
         * @return step name.
         */
        String stepName();

        /**
         * New workflow version to record (semver, e.g. {@code "0.0.2"}). Must be strictly greater than the workflow's
         * current version; downgrades raise {@link IllegalArgumentException}.
         *
         * @return new version.
         */
        String newVersion();

        /**
         * Event name customizer used to produce the wire-level name of the migration step event.
         *
         * @return event name customizer.
         */
        EventNameCustomizer eventNameCustomizer();
    }
}
