/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.saga;

import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Command payload dedicated to triggering the resolution of parameters through a custom
 * {@link ParameterResolverFactory}.
 *
 * @author Steven van Beelen
 */
public class ResolveParameterCommand {

    private final Object identifier;
    private final AtomicBoolean assertion;

    ResolveParameterCommand(Object identifier, AtomicBoolean assertion) {
        this.identifier = identifier;
        this.assertion = assertion;
    }

    public Object getIdentifier() {
        return identifier;
    }

    public AtomicBoolean getAssertion() {
        return assertion;
    }
}
