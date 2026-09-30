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
 * Spring configuration for the Axon Framework 4 Sagas carried by {@code axon-legacy}. Discovers
 * {@link org.axonframework.spring.stereotype.Saga @Saga}-annotated beans and assembles the event processors that
 * carry them.
 */
@NullMarked
package org.axonframework.extension.spring.config;

import org.jspecify.annotations.NullMarked;
