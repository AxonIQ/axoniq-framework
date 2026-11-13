/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;

import java.util.function.Consumer;

/**
 * Definition of the workflow.
 *
 * @param <T> type of the workflow context.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface WorkflowDefinition<T extends WorkflowContext> extends Consumer<T> {

}
