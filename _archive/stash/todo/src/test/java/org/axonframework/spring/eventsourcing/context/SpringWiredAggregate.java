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

package org.axonframework.spring.eventsourcing.context;

import org.axonframework.extension.spring.stereotype.EventSourced;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import org.jspecify.annotations.NonNull;

/**
 * Plain aggregate wired through Spring with the {@link EventSourced} annotation.
 *
 * @author Allard Buijze
 */
@EventSourced
public class SpringWiredAggregate implements ApplicationContextAware {

    private transient ApplicationContext context;

    public SpringWiredAggregate() {
    }

    public ApplicationContext getContext() {
        return context;
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
}
