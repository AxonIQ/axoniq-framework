/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
