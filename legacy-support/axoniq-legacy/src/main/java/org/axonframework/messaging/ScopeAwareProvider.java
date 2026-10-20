/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.messaging;

import java.util.stream.Stream;

/**
 * Contract towards a mechanism to provide a {@link Stream} of components which are {@link ScopeAware}.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public interface ScopeAwareProvider {

    /**
     * Retrieve a {@link Stream} of {@link ScopeAware} components, by performing a check whether that component is able
     * to handle a {@link Scope} described by a {@link ScopeDescriptor}.
     *
     * @param scopeDescriptor a {@link ScopeDescriptor} describing the {@link Scope} a component {@link ScopeAware}
     *                        should be able to handle
     * @return a {@link Stream} of {@link ScopeAware} components
     */
    Stream<ScopeAware> provideScopeAwareStream(ScopeDescriptor scopeDescriptor);
}
