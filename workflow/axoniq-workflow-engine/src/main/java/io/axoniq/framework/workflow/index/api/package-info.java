/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * The {@link io.axoniq.framework.workflow.index.api.WorkflowIndex} a workflow engine keeps its per-segment index of
 * running workflow instances in, so claiming a segment does not require reading every workflow lifecycle event.
 *
 * @since 5.5.0
 */
@NullMarked
package io.axoniq.framework.workflow.index.api;

import org.jspecify.annotations.NullMarked;
