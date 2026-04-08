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

package org.axonframework.modelling.saga;

/**
 * Interface describing a mechanism to inject resources into Saga instances.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public interface ResourceInjector {

    /**
     * Inject required resources into the given {@code saga}.
     *
     * @param saga The saga to inject resources into
     */
    void injectResources(Object saga);

}
