/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Function;

/**
 * Responsible for delivery of workflow id from provided event message.
 *
 * @author Simon Zambrovski.
 * @since 5.4.0
 */
@FunctionalInterface
public interface WorkflowIdProvider extends Function<EventMessage, String> {

}
