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

package io.axoniq.framework.axonserver.connector.event;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultPersistentStreamEventSourceFactory}.
 */
@ExtendWith(MockitoExtension.class)
class DefaultPersistentStreamEventSourceFactoryTest {

    private static final String STREAM_NAME = "myStream";

    @Mock
    private Configuration configuration;
    @Mock
    private ScheduledExecutorService scheduler;

    private DefaultPersistentStreamEventSourceFactory factory;
    private PersistentStreamProperties properties;

    @BeforeEach
    void setUp() {
        when(configuration.getComponent(AxonServerConnectionManager.class)).thenReturn(mock(AxonServerConnectionManager.class));
        when(configuration.getComponent(AxonServerConfiguration.class)).thenReturn(mock(AxonServerConfiguration.class));
        when(configuration.getComponent(EventConverter.class)).thenReturn(mock(EventConverter.class));
        when(configuration.getComponent(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));

        factory = new DefaultPersistentStreamEventSourceFactory();
        properties = new PersistentStreamProperties(STREAM_NAME, 1, "SequentialPerAggregate", Collections.emptyList(), "TAIL", null);
    }

    @Nested
    class Build {

        @Test
        void returnsEventSource() {
            // when
            PersistentStreamEventSource result = factory.build(STREAM_NAME, properties, scheduler, 1, configuration);

            // then
            assertThat(result).isNotNull();
        }

        @Test
        void returnsDistinctInstancesForSameName() {
            // when
            PersistentStreamEventSource first = factory.build(STREAM_NAME, properties, scheduler, 1, configuration);
            PersistentStreamEventSource second = factory.build(STREAM_NAME, properties, scheduler, 1, configuration);

            // then
            assertThat(first).isNotSameAs(second);
        }
    }

    @Nested
    class DuplicateStreamNameWarning {

        private ListAppender<ILoggingEvent> logAppender;

        @BeforeEach
        void attachAppender() {
            logAppender = new ListAppender<>();
            logAppender.start();
            ((Logger) LoggerFactory.getLogger(DefaultPersistentStreamEventSourceFactory.class)).addAppender(logAppender);
        }

        @AfterEach
        void detachAppender() {
            ((Logger) LoggerFactory.getLogger(DefaultPersistentStreamEventSourceFactory.class)).detachAppender(logAppender);
        }

        @Test
        void noWarningOnFirstBuild() {
            // when
            factory.build(STREAM_NAME, properties, scheduler, 1, configuration);

            // then
            assertThat(logAppender.list)
                    .noneMatch(event -> event.getLevel() == Level.WARN);
        }

        @Test
        void warnsWhenSameStreamNameUsedTwice() {
            // given
            factory.build(STREAM_NAME, properties, scheduler, 1, configuration);

            // when
            factory.build(STREAM_NAME, properties, scheduler, 1, configuration);

            // then
            assertThat(logAppender.list)
                    .anyMatch(event -> event.getLevel() == Level.WARN
                            && event.getFormattedMessage().contains(STREAM_NAME));
        }

        @Test
        void noWarningForDifferentStreamNames() {
            // when
            PersistentStreamProperties otherProps =
                    new PersistentStreamProperties("otherStream", 1, "SequentialPerAggregate", Collections.emptyList(), "TAIL", null);
            factory.build(STREAM_NAME, properties, scheduler, 1, configuration);
            factory.build("otherStream", otherProps, scheduler, 1, configuration);

            // then
            assertThat(logAppender.list)
                    .noneMatch(event -> event.getLevel() == Level.WARN);
        }
    }
}
