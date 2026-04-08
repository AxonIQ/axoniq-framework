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

import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.event.EventListener;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;

import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
public class ApplicationContextEventPublisherTest {

    @Autowired
    private ListenerBean listenerBean;

    @Autowired
    private EventBus eventBus;

    @Test
    void eventsForwardedToListenerBean() {
        eventBus.publish(null, asEventMessage("test"));

        assertEquals("test", listenerBean.getEvents().get(0));
    }

    @Configuration
    public static class Context {

        @Bean
        public ListenerBean listenerBean() {
            return new ListenerBean();
        }

        @Bean
        public EventBus eventBus() {
            return new SimpleEventBus();
        }

        @Bean
        public ApplicationContextEventPublisher publisher(EventBus eventBus) {
            return new ApplicationContextEventPublisher(eventBus);
        }
    }

    public static class ListenerBean {

        private List<Object> events = new ArrayList<>();

        @EventListener
        public void handle(PayloadApplicationEvent<String> event) {
            events.add(event.getPayload());
        }

        public List<Object> getEvents() {
            return events;
        }
    }
}
