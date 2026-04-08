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
 * Exception indicating that a duplicate registration of modules has been detected. This happens when two modules are
 * registered under the same name.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public class DuplicateModuleRegistrationException extends RuntimeException {

    /**
     * Initialize the exception indicating that the given {@code module} failed to register.
     *
     * @param module The module that failed to register.
     */
    public DuplicateModuleRegistrationException(Module module) {
        super("A module with given name already exists: " + module.name());
    }
}
