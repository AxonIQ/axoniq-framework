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
 * Public tracing SPI for AxoniqFramework: the single {@link io.axoniq.framework.tracing.SpanFactory}
 * abstraction together with {@link io.axoniq.framework.tracing.Span},
 * {@link io.axoniq.framework.tracing.SpanScope}, the
 * {@link io.axoniq.framework.tracing.SpanAttributesProvider} SPI and the built-in span factories.
 */
@NullMarked
package io.axoniq.framework.tracing;

import org.jspecify.annotations.NullMarked;
