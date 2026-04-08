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

package org.axonframework.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker to specify the registration scope for objects that are registered in the registry.
 * <p>
 * Whenever the scope is set to {@link Scope#CURRENT}, the annotated component will never be copied to a child registry
 * once it's created. Using the scope {@link Scope#CHILDREN} will ensure the annotated component is copied to a child
 * registry (and later to its children), once it's created.
 * <p>
 * This annotation is intended to be put on {@link org.axonframework.common.configuration.ConfigurationEnhancer} and
 * {@link org.axonframework.common.configuration.DecoratorDefinition} classes to control their registration scopes.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
public @interface RegistrationScope {

    /**
     * Optional description of the reason.
     *
     * @return A description of the reason.
     */
    String value() default "";

    /**
     * Sets the registration scope for the instance.
     *
     * @return The registration scope.
     */
    Scope scope() default Scope.CURRENT;

    /**
     * Registration scope.
     */
    enum Scope {
        /**
         * Register only for current registry.
         */
        CURRENT,
        /**
         * Register for current registry, but copy to all child registries, created from it.
         */
        CHILDREN
    }
}
