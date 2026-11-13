/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.messaging;

import java.io.Serializable;

/**
 * Denotes the description of a {@link Scope}. This can be used to figure out in what scope a given message should be
 * handled.
 * <p>
 * Descriptors are {@link Serializable}, as in Axon Framework 4: a deadline manager stores the descriptor of the scope a
 * deadline belongs to with the deadline, and a converter that relies on Java serialization semantics, such as the
 * {@link org.axonframework.conversion.xstream.XStreamConverter}, then writes them in the same form Axon Framework 4
 * stored.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public interface ScopeDescriptor extends Serializable {

    /**
     * Retrieve a {@link String} description of a {@link Scope} object.
     *
     * @return a {@link String} description of a {@link Scope} object
     */
    String scopeDescription();
}
