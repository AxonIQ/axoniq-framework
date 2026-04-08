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

import org.axonframework.messaging.core.annotation.HasHandlerAttributes;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to specify the creation policy for a command handler. Default behavior is that command handlers
 * defined on a constructor would create a new instance of the aggregate, and command handlers defined on other methods
 * expect an existing aggregate. This annotation provides the option to define policy {@code
 * AggregateCreationPolicy.CREATE_IF_MISSING} or {@code AggregateCreationPolicy.ALWAYS} on a command handler to create a
 * new instance of the aggregate from a handler operation.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface CreationPolicy {

    /**
     * Specifies the {@link AggregateCreationPolicy} to apply. {@code NEVER} when not set.
     *
     * @return the creation policy
     */
    AggregateCreationPolicy value() default AggregateCreationPolicy.NEVER;
}
