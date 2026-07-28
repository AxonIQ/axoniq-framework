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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The framework emits exactly one DEBUG log entry when a chain is built, listing the
 * registered transformations.
 */
final class EventTransformerChainBuildLogTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    private Logger chainLogger;
    private Level previousLevel;
    private ListAppender appender;

    @BeforeEach
    void attachAppender() {
        chainLogger = (Logger) LogManager.getLogger(EventTransformerChain.class);
        previousLevel = chainLogger.getLevel();
        chainLogger.setLevel(Level.DEBUG);
        appender = new ListAppender("EventTransformerChainBuildLog");
        appender.start();
        chainLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        chainLogger.removeAppender(appender);
        appender.stop();
        chainLogger.setLevel(previousLevel);
    }

    @Test
    void buildEmitsExactlyOneDebugLineNamingEachRegisteredTransformation() {
        EventTransformation v1ToV2 = EventTransformation.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> in);

        EventTransformerChain.builder().register(v1ToV2).build();

        List<LogEvent> debugEvents = appender.getEvents().stream()
                .filter(event -> event.getLevel() == Level.DEBUG)
                .toList();
        assertThat(debugEvents)
                .as("exactly one DEBUG entry MUST be emitted per chain build")
                .hasSize(1);
        String formatted = debugEvents.getFirst().getMessage().getFormattedMessage();
        assertThat(formatted)
                .contains("EventTransformerChain built with 1 transformation(s)")
                .contains(V1.toString())
                .contains(V2.toString());
    }

    @Test
    void buildEmitsADedicatedNoOpEntryWhenZeroTransformationsAreRegistered() {
        EventTransformerChain.builder().build();

        List<LogEvent> debugEvents = appender.getEvents().stream()
                .filter(event -> event.getLevel() == Level.DEBUG)
                .toList();
        assertThat(debugEvents).hasSize(1);
        assertThat(debugEvents.getFirst().getMessage().getFormattedMessage())
                .contains("0 transformations")
                .contains("no-op pass-through");
    }

    @Test
    void buildEmitsNoLogWhenDebugIsDisabled() {
        chainLogger.setLevel(Level.WARN);
        EventTransformation v1ToV2 = EventTransformation.from(V1).to(V2)
                                                     .transform(JsonNode.class, (in, ctx) -> in);

        EventTransformerChain.builder().register(v1ToV2).build();

        assertThat(appender.getEvents()).isEmpty();
    }
}
