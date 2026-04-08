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

package org.axonframework.eventsourcing.annotation;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.modelling.annotation.TargetEntityId;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate that a method can be used to resolve the {@link EventCriteria} based on the
 * {@link TargetEntityId} when loading an {@link EventSourcedEntity}.
 * <p>
 * The method should be a static method that returns an {@link EventCriteria} instance. The first argument should be the
 * identifier of the entity to load. If you need to resolve multiple identifier types, you can use the
 * {@link EventCriteriaBuilder} annotation on multiple methods.
 * <p>
 * You can define any component from the {@link Configuration} as a parameter to the
 * method to be able to resolve the {@link EventCriteria}. You can also inject the entire configuration as a parameter
 * by declaring it as such. Note that the first parameter must be the identifier, and cannot be a component.
 *
 * @author Mitchell Herrijgers
 * @see TargetEntityId
 * @see EventSourcedEntity
 * @since 5.0.0
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EventCriteriaBuilder {

}
