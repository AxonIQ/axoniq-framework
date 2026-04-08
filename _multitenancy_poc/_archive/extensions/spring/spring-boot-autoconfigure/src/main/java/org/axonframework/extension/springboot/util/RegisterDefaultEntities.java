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

package org.axonframework.extension.springboot.util;

import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation intended for types and methods to trigger the registration of entities found in the given
 * {@link #packages()} with the {@link DefaultEntityRegistrar}.
 *
 * @author Allard Buijze
 * @since 3.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(DefaultEntityRegistrar.class)
public @interface RegisterDefaultEntities {

    /**
     * An array of package names used by the {@link DefaultEntityRegistrar} to collect entities from.
     *
     * @return An array of package names used by the {@link DefaultEntityRegistrar} to collect entities from.
     */
    String[] packages();
}
