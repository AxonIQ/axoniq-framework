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

package org.axonframework.messaging.eventhandling.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation indication that a parameter on an Event Handler method should be injected with the Timestamp of an Event
 * Message. The parameter type must be assignable from {@link java.time.Instant}.
 *
 * @author Allard Buijze
 * @since 2.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface Timestamp {

}
