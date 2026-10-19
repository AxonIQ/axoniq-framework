/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.integrationtests.deadline.jobrunr;

import org.jobrunr.server.JobActivator;

/**
 * When using Spring it will use the context to get the bean, this is a simple activator, just to return the manager
 * instance. This keeps the tests light.
 */
public class SimpleActivator<T> implements JobActivator {

    private final T instance;

    public SimpleActivator(T instance) {
        this.instance = instance;
    }

    @Override
    public <T> T activateJob(Class<T> type) {
        if (type.isAssignableFrom(instance.getClass())) {
            return (T) instance;
        }
        return null;
    }
}
