/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Internal runtime contracts for workflow execution, command dispatch, configuration, and event publication.
 * Author-facing step definitions and event conditions reside in {@link WorkflowContext}'s DSL package and are
 * translated into these runtime contracts.
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.jspecify.annotations.NullMarked;
