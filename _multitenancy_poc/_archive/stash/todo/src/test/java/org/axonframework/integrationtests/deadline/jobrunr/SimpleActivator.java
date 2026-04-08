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
