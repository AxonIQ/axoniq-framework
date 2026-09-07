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
package io.axoniq.framework.workflow.simulation.harness;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records the messages one logger emits while attached. Scenarios use it to observe the engine's own rejection
 * warnings, the fault-landed proof that a fence fired. Close it to detach.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class LogCapture implements AutoCloseable {

    private final LoggerContext context;
    private final LoggerConfig loggerConfig;
    private final Recorder recorder = new Recorder();

    private LogCapture(String loggerName) {
        context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        LoggerConfig nearest = configuration.getLoggerConfig(loggerName);
        if (nearest.getName().equals(loggerName)) {
            loggerConfig = nearest;
        } else {
            loggerConfig = new LoggerConfig(loggerName, nearest.getLevel(), true);
            configuration.addLogger(loggerName, loggerConfig);
        }
        recorder.start();
        loggerConfig.addAppender(recorder, Level.ALL, null);
        context.updateLoggers();
    }

    /**
     * Starts recording everything the named logger emits at its effective level.
     *
     * @param loggerName the logger to observe
     * @return the capture; close it to stop recording
     */
    public static LogCapture attach(String loggerName) {
        return new LogCapture(loggerName);
    }

    /**
     * @return a snapshot of the formatted messages recorded so far
     */
    public List<String> messages() {
        return recorder.events.stream()
                              .map(event -> event.getMessage().getFormattedMessage())
                              .toList();
    }

    /**
     * @param level the level to keep
     * @return a snapshot of the formatted messages recorded so far at exactly the given level
     */
    public List<String> messages(Level level) {
        return recorder.events.stream()
                              .filter(event -> event.getLevel() == level)
                              .map(event -> event.getMessage().getFormattedMessage())
                              .toList();
    }

    @Override
    public void close() {
        loggerConfig.removeAppender(recorder.getName());
        context.updateLoggers();
        recorder.stop();
    }

    private static final class Recorder extends AbstractAppender {

        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        private Recorder() {
            super("LogCapture-" + UUID.randomUUID(), null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }
}
