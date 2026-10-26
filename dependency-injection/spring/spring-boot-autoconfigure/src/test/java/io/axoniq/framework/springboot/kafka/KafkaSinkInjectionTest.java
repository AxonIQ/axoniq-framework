/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.springboot.kafka;

import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shows which {@link EventSink} a Spring application receives where, when a Kafka sink is configured. Configuring a
 * sink does not connect to Kafka, and nothing is published to it here, so no broker is needed.
 * <p>
 * It characterizes a limitation of Axon Framework rather than desired behavior: an {@link EventSink} parameter of an
 * event handler method is resolved by type, by the framework's {@code HierarchicalParameterResolverFactory}, before
 * Spring's qualifier-aware parameter resolution is consulted, so a {@code @Qualifier} on that parameter is ignored.
 * The qualified sink is therefore injected into the handling component instead.
 *
 * @author Allard Buijze
 */
@SpringBootTest(classes = KafkaSinkInjectionTest.TestApplication.class, properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "spring.main.banner-mode=off",
        "axon.kafka.sinks.outbound.topic=parcels"
})
class KafkaSinkInjectionTest {

    @Autowired
    private EventGateway eventGateway;

    @Autowired
    private EventSink applicationSink;

    @Autowired
    @Qualifier("outbound")
    private EventSink kafkaSink;

    @Autowired
    private ParcelHandler handler;

    @Event(namespace = "parcels", name = "ParcelRegistered", version = "1.0")
    public record ParcelRegistered(String parcelId) {

    }

    /**
     * Records the sink it received through its constructor, and the sinks its handler receives as parameters.
     */
    public static class ParcelHandler {

        final EventSink constructorSink;
        final List<EventSink> qualifiedParameterSinks = new CopyOnWriteArrayList<>();

        ParcelHandler(EventSink constructorSink) {
            this.constructorSink = constructorSink;
        }

        @EventHandler
        public void on(ParcelRegistered event, @Qualifier("outbound") EventSink sink) {
            qualifiedParameterSinks.add(sink);
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {JmxAutoConfiguration.class, WebClientAutoConfiguration.class})
    static class TestApplication {

        @Bean
        ParcelHandler parcelHandler(@Qualifier("outbound") EventSink kafka) {
            return new ParcelHandler(kafka);
        }
    }

    @Test
    void qualifiedConstructorParameterReceivesTheKafkaSink() {
        // then
        assertThat(handler.constructorSink).isSameAs(kafkaSink)
                                           .isNotSameAs(applicationSink);
    }

    @Test
    void qualifierOnAnEventHandlerParameterIsNotHonoured() {
        // when
        eventGateway.publish(null, new ParcelRegistered("p-1")).orTimeout(10, TimeUnit.SECONDS).join();

        // then - the processor's configuration resolves the parameter by type, before Spring's qualifier-aware
        // resolution is consulted; the reference guide warns about this
        await().atMost(Duration.ofSeconds(10)).until(() -> !handler.qualifiedParameterSinks.isEmpty());
        assertThat(handler.qualifiedParameterSinks).allSatisfy(sink -> assertThat(sink).isSameAs(applicationSink));
    }
}
