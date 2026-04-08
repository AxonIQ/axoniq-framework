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
 * Enumeration describing how a {@link ComponentRegistry} should react when a {@link Component} is to be overridden
 * during a {@link ComponentRegistry#registerComponent(ComponentDefinition)} invocation.
 *
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @author Allard Buijze
 * @author Mateusz Nowak
 * @see ComponentRegistry#setOverridePolicy(OverridePolicy)
 * @since 5.0.0
 */
public enum OverridePolicy {
    /**
     * Overriding is allowed at all times.
     */
    ALLOW,
    /**
     * Overriding a components results in a WARN-level log message.
     */
    WARN,
    /**
     * Trying to override results in a {@link ComponentOverrideException}.
     */
    REJECT
}
