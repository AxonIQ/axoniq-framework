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

package org.axonframework.messaging.core;

/**
 * A {@link ScopeDescriptor} describing no active scope.
 *
 * @author Steven van Beelen
 * @since 4.5
 */
public class NoScopeDescriptor implements ScopeDescriptor {

    /**
     * A statically available instance of the {@link NoScopeDescriptor}.
     */
    public static final NoScopeDescriptor INSTANCE = new NoScopeDescriptor();

    private NoScopeDescriptor() {
    }

    @Override
    public String scopeDescription() {
        return "NoActiveScope";
    }
}
