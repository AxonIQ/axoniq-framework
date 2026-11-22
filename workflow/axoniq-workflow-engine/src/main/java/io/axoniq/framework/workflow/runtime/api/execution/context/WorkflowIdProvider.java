/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Function;

/**
 * Derives the id of a new workflow instance from the event that starts it.
 * <p>
 * The id must be valid: at least one and at most 255 characters, each of them printable ASCII ({@code 0x20} to
 * {@code 0x7E}, the space included), so an empty id is invalid. The workflow engine applies these rules whether or
 * not a workflow index is configured. When the derived id is invalid, or the provider derives none ({@code null}), the
 * engine does not start the instance and logs an error naming the workflow definition and the event; the event is
 * handled as usual otherwise. To start workflows for a longer or non-ASCII business key, derive a valid id from it, for
 * example the SHA-256 hash of the key as 64 hexadecimal characters.
 * <p>
 * The default provider, {@link io.axoniq.framework.workflow.runtime.execution.MessageWorkflowIdProvider}, uses the
 * identifier of the event: a UUID, and therefore valid, unless the application assigns identifiers of its own. The
 * {@link io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider} reads the id from a payload
 * property, optionally transforming it.
 *
 * @author Simon Zambrovski.
 * @since 5.4.0
 */
@FunctionalInterface
public interface WorkflowIdProvider extends Function<EventMessage, String> {

}
