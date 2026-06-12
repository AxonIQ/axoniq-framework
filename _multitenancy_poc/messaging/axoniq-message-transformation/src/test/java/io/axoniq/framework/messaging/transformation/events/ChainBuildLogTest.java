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

package io.axoniq.framework.messaging.transformation.events;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The framework emits exactly one INFO log entry when a chain is built, listing the
 * registered transformations.
 */
final class ChainBuildLogTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    private Logger chainLogger;
    private Level previousLevel;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachAppender() {
        chainLogger = (Logger) LoggerFactory.getLogger(EventTransformerChain.class);
        previousLevel = chainLogger.getLevel();
        chainLogger.setLevel(Level.INFO);
        appender = new ListAppender<>();
        appender.start();
        chainLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        chainLogger.detachAppender(appender);
        appender.stop();
        chainLogger.setLevel(previousLevel);
    }

    @Test
    void buildEmitsExactlyOneInfoLineNamingEachRegisteredTransformer() {
        EventTransformer v1ToV2 = EventTransformer.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> in);

        EventTransformerChain.builder().register(v1ToV2).build();

        List<ILoggingEvent> infoEvents = appender.list.stream()
                .filter(event -> event.getLevel() == Level.INFO)
                .toList();
        assertThat(infoEvents)
                .as("exactly one INFO entry MUST be emitted per chain build")
                .hasSize(1);
        String formatted = infoEvents.getFirst().getFormattedMessage();
        assertThat(formatted)
                .contains("EventTransformerChain built with 1 transformation(s)")
                .contains(V1.toString())
                .contains(V2.toString());
    }

    @Test
    void buildEmitsADedicatedNoOpEntryWhenZeroTransformersAreRegistered() {
        EventTransformerChain.builder().build();

        List<ILoggingEvent> infoEvents = appender.list.stream()
                .filter(event -> event.getLevel() == Level.INFO)
                .toList();
        assertThat(infoEvents).hasSize(1);
        assertThat(infoEvents.getFirst().getFormattedMessage())
                .contains("0 transformations")
                .contains("no-op pass-through");
    }

    @Test
    void buildEmitsNoLogWhenInfoIsDisabled() {
        chainLogger.setLevel(Level.WARN);
        EventTransformer v1ToV2 = EventTransformer.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> in);

        EventTransformerChain.builder().register(v1ToV2).build();

        assertThat(appender.list).isEmpty();
    }
}
