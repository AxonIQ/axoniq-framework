/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.axonframework.common.AxonException;

/**
 * Indicates that the {@link org.bson.Document} did not contain the keys expected of an {@link InstantEntry}.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class NotAnInstantDocumentException extends AxonException {

    /**
     * Constructs a {@code NotAnInstantDocumentException} with the provided {@code message}.
     *
     * @param message The message containing more details about the cause.
     */
    public NotAnInstantDocumentException(String message) {
        super(message);
    }
}
