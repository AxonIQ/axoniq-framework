/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Utilities for evaluating workflow-state queries against in-memory workflow state. Query-capable repositories should
 * translate queries to their native storage model instead of materializing state for evaluation.
 */
@NullMarked
package io.axoniq.framework.workflow.query.utils;

import org.jspecify.annotations.NullMarked;
