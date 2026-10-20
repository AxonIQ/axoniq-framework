/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Tests for the workflow execute-step retry policy API.
 *
 * <p>The production retry contracts reside in {@code execution.context.retry}. These tests verify retry decisions,
 * retry context values, and backoff strategy behavior independently from workflow execution.</p>
 */
@NullMarked
package io.axoniq.framework.workflow.dsl.api.retry;

import org.jspecify.annotations.NullMarked;
