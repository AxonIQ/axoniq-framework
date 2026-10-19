/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.messaging;

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
