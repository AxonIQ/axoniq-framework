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

package org.axonframework.eventsourcing.annotation.reflection;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated parameter should be injected with the entity identifier of the
 * {@link EntityCreator}-annotated method or constructor being invoked.
 * <p>
 * This annotation is necessary due to potential ambiguities, where, for example, both the id and the payload of the
 * first event can be a {@link String}. As throughout the framework the first parameter without an annotation is assumed
 * to be the payload, this annotation is used to indicate that the parameter should be injected with the entity
 * identifier instead.
 *
 * @author Mitchell Herrijgers
 * @see EntityCreator
 * @since 5.0.0
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectEntityId {

}
