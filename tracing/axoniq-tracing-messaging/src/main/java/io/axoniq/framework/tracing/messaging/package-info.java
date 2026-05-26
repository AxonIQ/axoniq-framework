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
 * Tracing wiring for {@code axon-messaging} components. The {@code internal} sub-package holds the
 * delegating tracing decorators; they are registered with the framework by the
 * ServiceLoader-discovered {@code MessagingTracingConfigurationEnhancer}.
 */
@NullMarked
package io.axoniq.framework.tracing.messaging;

import org.jspecify.annotations.NullMarked;
