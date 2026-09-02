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
 * Spring Boot configuration properties for the Axoniq Framework Spring Cloud modules.
 * <p>
 * Sits under {@code io.axoniq.framework.springboot} with the framework's other Spring Boot wiring, rather than under
 * the connector's own package, so that nothing here is a dependency of the connector on its own consumers.
 */
@NullMarked
package io.axoniq.framework.springboot.springcloud;

import org.jspecify.annotations.NullMarked;
