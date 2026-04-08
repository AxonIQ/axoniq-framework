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

package org.axonframework.extension.spring.messaging;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.PayloadApplicationEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Component that forward events received from a {@link SubscribableEventSource} as Spring {@link ApplicationEvent} to
 * the ApplicationContext.
 */
public class ApplicationContextEventPublisher implements InitializingBean, ApplicationContextAware {

    private final SubscribableEventSource messageSource;
    private ApplicationContext applicationContext;

    /**
     * Initialize the publisher to forward events received from the given {@code messageSource} to the application
     * context that this bean is part of.
     *
     * @param messageSource The source to subscribe to.
     */
    public ApplicationContextEventPublisher(SubscribableEventSource messageSource) {
        this.messageSource = messageSource;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Converts the given Axon {@code eventMessage} to a Spring ApplicationEvent. This method may be overridden to
     * change the translation.
     * <p>
     * The default implementation creates a {@link PayloadApplicationEvent} with the Message's payload.
     * <p>
     * If this method returns {@code null}, no message is published
     *
     * @param eventMessage The EventMessage to transform
     * @return the Spring ApplicationEvent representing the Axon EventMessage
     */
    protected ApplicationEvent convert(EventMessage eventMessage) {
        return new PayloadApplicationEvent<>(messageSource, eventMessage.payload());
    }

    @Override
    public void afterPropertiesSet() {
        messageSource.subscribe((msgs, ctx) -> {
            msgs.forEach(msg -> {
                ApplicationEvent converted = convert(msg);
                if (converted != null) {
                    applicationContext.publishEvent(convert(msg));
                }
            });
            return CompletableFuture.completedFuture(null);
        });
    }
}
