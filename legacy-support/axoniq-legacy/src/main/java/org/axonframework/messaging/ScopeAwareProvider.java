/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
