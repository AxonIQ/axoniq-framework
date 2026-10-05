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

/**
 * Denotes the description of a {@link Scope}. This can be used to figure out in what scope a given message should be
 * handled.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public interface ScopeDescriptor {

    /**
     * Retrieve a {@link String} description of a {@link Scope} object.
     *
     * @return a {@link String} description of a {@link Scope} object
     */
    String scopeDescription();
}
