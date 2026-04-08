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

package org.axonframework.common.configuration;

/**
 * Enumeration stating on what levels to search for a {@link Component} within the {@link ComponentRegistry}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public enum SearchScope {

    /**
     * Searches for {@link Component Components} in the current {@link ComponentRegistry} <b>and</b> any ancestors of
     * the current registry.
     */
    ALL,

    /**
     * Searches for {@link Component Components} in the current {@link ComponentRegistry} only, disregarding any
     * ancestors.
     */
    CURRENT,

    /**
     * Searches for {@link Component Components} in the ancestors of the current {@link ComponentRegistry} only.
     */
    ANCESTORS;
}
