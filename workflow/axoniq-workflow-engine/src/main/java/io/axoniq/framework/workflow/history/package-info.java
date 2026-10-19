/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Facilities for retaining and reading completed workflow execution history.
 *
 * <p>The {@code api} subpackage defines the history model and repository abstraction. Implementations, such as the
 * in-memory projector, translate workflow events into history records that can be queried after execution ends.</p>
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.history;

import org.jspecify.annotations.NullMarked;
