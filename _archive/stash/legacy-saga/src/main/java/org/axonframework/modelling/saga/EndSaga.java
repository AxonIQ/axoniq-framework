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

package org.axonframework.modelling.saga;

import org.axonframework.messaging.core.annotation.HasHandlerAttributes;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated {@link SagaEventHandler} method indicates the end of a Saga instance's lifecycle. When
 * event handling completes, the Saga is destroyed and may no longer receive events.
 * <p/>
 * This annotation can only appear on methods that have been annotated with {@link SagaEventHandler}.
 *
 * @author Allard Buijze
 * @since 0.7
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface EndSaga {

}
