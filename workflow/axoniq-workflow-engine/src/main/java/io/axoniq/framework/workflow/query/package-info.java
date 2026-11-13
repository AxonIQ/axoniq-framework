/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * Query model and evaluation support for selecting workflow state.
 *
 * <p>The {@code api} subpackage provides value-based query criteria. Utility implementations evaluate those criteria
 * against materialized state, while storage-backed repositories can translate them to their native query model.</p>
 *
 * @since 5.4.0
 */
@NullMarked
package io.axoniq.framework.workflow.query;

import org.jspecify.annotations.NullMarked;
