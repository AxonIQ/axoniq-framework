/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Internal implementation of the workflow execution engine: the {@code WorkflowEngine}, its scheduling, state and
 * storage, checkpointing/replay support, cancellation coordination, and the delegates implementing individual step
 * primitives.
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.runtime.execution;

import org.jspecify.annotations.NullMarked;
