/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.api.payload;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Action executed consuming payload and returning payload as a result run in a provided processing context.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@FunctionalInterface
public interface PayloadProcessor extends BiFunction<ProcessingContext, Map<String, Object>, Map<String, Object>> {

}
