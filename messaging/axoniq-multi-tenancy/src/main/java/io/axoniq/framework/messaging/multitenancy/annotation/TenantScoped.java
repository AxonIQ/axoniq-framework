/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation indication that a parameter on a Message Handler method should be injected with a
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider tenant-scoped instance} of a component.
 * The parameter type must be resolvable through a
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider} registered in the configuration.
 *
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface TenantScoped {

}
