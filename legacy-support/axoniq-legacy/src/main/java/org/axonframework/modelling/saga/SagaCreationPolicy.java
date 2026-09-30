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

package org.axonframework.modelling.saga;

/**
 * Enumeration containing the possible Creation Policies for Sagas.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public enum SagaCreationPolicy {

    /**
     * Never create a new Saga instance, even if none exists.
     */
    NONE,

    /**
     * Only create a new Saga instance if none can be found.
     */
    IF_NONE_FOUND,

    /**
     * Always create a new Saga, even if one already exists.
     */
    ALWAYS
}
