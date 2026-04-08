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

import java.lang.annotation.*;

/**
 * Annotation placed on types that should be treated as the root of an aggregate. Such types will be the entry point for
 * command messages that target the aggregate.
 * <p>
 * The use of this annotation is not mandatory for the framework. It allows setting an explicit type name for the
 * Aggregate. Other than that, it's probably useful for developers.
 */
@Documented
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateRoot {
    /**
     * Get the String representation of the aggregate's type. Optional. This defaults to the simple name of the
     * annotated class.
     */
    String type() default "";
}
