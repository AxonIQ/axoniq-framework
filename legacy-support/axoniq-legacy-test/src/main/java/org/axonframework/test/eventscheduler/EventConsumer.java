/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.eventscheduler;

/**
 * A consumer that allows implementations to throw exceptions.
 *
 * @author Steve Chernyak
 * @since 3.0
 */
@FunctionalInterface
public interface EventConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param var the input argument
     * @throws Exception if the operation results in an exception
     */
    void accept(T var);
}
