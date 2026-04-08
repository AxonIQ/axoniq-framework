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

package org.axonframework.modelling.command;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link java.lang.reflect.Field} or {@link java.lang.reflect.Method} annotation that marks the member containing the
 * identifier of an Entity. Commands for a child Entity are routed to the Entity if the value of the Command's {@link
 * #routingKey()} property matches the value of the annotated field.
 *
 * @author Allard Buijze
 * @since 3.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.FIELD})
public @interface EntityId {

    /**
     * Get the name of the routing key property on commands and events that provides the identifier that should be used
     * to target the entity with the annotated member.
     * <p>
     * Optional. If left empty this defaults to the member name. If the member was named in a "getter" style, the {@code
     * "get"} will be removed.
     * <p>
     * Setting the {@code routingKey} is especially useful for annotated {@link java.lang.reflect.Method}s, which
     * typically have a different naming scheme than a field in a command/event.
     */
    String routingKey() default "";
}
