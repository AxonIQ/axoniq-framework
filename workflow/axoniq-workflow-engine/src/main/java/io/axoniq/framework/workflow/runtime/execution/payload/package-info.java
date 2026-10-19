/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * {@code PayloadReducer} implementations and their registry, combining or selecting between the global (workflow
 * context) and local payloads passed to and returned from workflow steps.
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.runtime.execution.payload;

import org.jspecify.annotations.NullMarked;
