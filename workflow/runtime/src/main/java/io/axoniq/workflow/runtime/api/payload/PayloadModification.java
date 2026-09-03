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
package io.axoniq.workflow.runtime.api.payload;

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

/**
 * Payload modification interface.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.1.0
 */
@FunctionalInterface
public interface PayloadModification extends Function<Map<String, @Nullable Object>, Map<String, @Nullable Object>> {

}
