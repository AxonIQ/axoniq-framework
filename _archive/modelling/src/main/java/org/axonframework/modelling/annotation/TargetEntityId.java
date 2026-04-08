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

package org.axonframework.modelling.annotation;

import org.axonframework.messaging.core.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be placed on a parameter of a field or method of the payload of a {@link Message}, which provides the
 * identifier of the target entity when using the {@link AnnotationBasedEntityIdResolver}.
 * <p>
 * See the {@link InjectEntity} annotation for more information about the different ways to resolve the entity id when
 * injecting entities into message handlers.
 * <p>
 * Multiple parameters annotated with {@link TargetEntityId} are allowed, but only one distinct non-null value may be
 * returned. If multiple non-null values are found that don't match, or no non-null values were found, a
 * {@link org.axonframework.modelling.EntityIdResolutionException} is thrown.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TargetEntityId {

}
