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
package io.axoniq.framework.extension.dataprotection.sample.config;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

/**
 * Service for managing event processor token resets.
 *
 * <p>This service provides functionality to reset the tracking tokens of streaming event processors,
 * effectively replaying all events from the tail of the event stream. This is useful for:</p>
 * <ul>
 *   <li>Rebuilding read models after encryption key deletion (GDPR forget)</li>
 *   <li>Recovering from projection errors</li>
 *   <li>Rebuilding projections after logic changes</li>
 * </ul>
 *
 * <p><strong>GDPR Integration:</strong></p>
 * <p>When an encryption key is deleted (GDPR "right to be forgotten"), the projections need to be
 * updated to reflect that the encrypted data is no longer accessible. This service is automatically
 * called after key deletion to reset the projection tokens and trigger a replay of all events,
 * ensuring that projections show the replacement values (e.g., "&lt;removed&gt;") for the deleted data.</p>
 *
 * <p><strong>⚠️ IMPORTANT - Axon Framework 5 Limitation:</strong></p>
 * <p>PooledStreamingEventProcessor does NOT support token reset in AF5.0-SNAPSHOT (throws
 * ResetNotSupportedException with message "TODO #3304"). Only TrackingEventProcessor supports
 * reset operations. To use this service, configure your event processors to use tracking mode
 * in application.properties:</p>
 * <pre>
 * axon.eventhandling.processors.{processorName}.mode=tracking
 * </pre>
 *
 * <p><strong>Warning:</strong> This operation will:</p>
 * <ol>
 *   <li>Shut down the event processor</li>
 *   <li>Reset all tracking tokens to the tail (latest position)</li>
 *   <li>Restart the event processor</li>
 * </ol>
 *
 * @see StreamingEventProcessor
 * @since 1.0
 */
@Service
public class EventProcessorResetService {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Configuration configuration;

    public EventProcessorResetService(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Resets all event processor tokens to trigger a full replay.
     *
     * <p>This method iterates through all configured event processors and resets
     * those that support reset operations. This is typically called after GDPR
     * encryption key deletion to ensure projections are updated with replacement values.</p>
     *
     * @return CompletableFuture that completes when all processors have been reset
     */
    public CompletableFuture<Void> resetAllProcessors() {
        logger.info("Resetting all event processors to trigger projection replay");

        // AF5: Use getComponents(EventProcessor.class) instead of eventProcessingConfiguration()
        var allProcessors = configuration.getComponents(EventProcessor.class);
        logger.info("Found {} total event processors", allProcessors.size());

        allProcessors.forEach((name, processor) -> {
            logger.info("Processor: {} - Type: {} - IsStreaming: {} - SupportsReset: {}",
                name,
                processor.getClass().getSimpleName(),
                processor instanceof StreamingEventProcessor,
                processor instanceof StreamingEventProcessor && ((StreamingEventProcessor) processor).supportsReset()
            );
        });

        return allProcessors.values()
                .stream()
                .filter(processor -> {
                    boolean isStreaming = processor instanceof StreamingEventProcessor;
                    if (!isStreaming) {
                        logger.debug("Skipping non-streaming processor: {}", processor.name());
                    }
                    return isStreaming;
                })
                .map(processor -> (StreamingEventProcessor) processor)
                .map(this::performReset)
                .reduce(CompletableFuture.completedFuture(null),
                        (f1, f2) -> f1.thenCompose(v -> f2))
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Error resetting event processors", throwable);
                    } else {
                        logger.info("All event processors reset successfully");
                    }
                });
    }

    /**
     * Triggers a token reset for the specified processing group.
     *
     * <p>This operation is asynchronous in Axon Framework 5. The method will:</p>
     * <ol>
     *   <li>Validate that the processing group name is not empty</li>
     *   <li>Look up the event processor by processing group name</li>
     *   <li>Check if the processor supports reset operations</li>
     *   <li>If supported, shut down → reset tokens to tail → restart</li>
     * </ol>
     *
     * <p><strong>AF5 Changes:</strong> All lifecycle methods now return {@link CompletableFuture}
     * for proper asynchronous handling. The operations are chained using {@code thenCompose()}.</p>
     *
     * @param processingGroup the name of the processing group to reset (must not be empty)
     * @return CompletableFuture that completes when the processor has been reset
     * @throws IllegalArgumentException if processingGroup is null or empty
     */
    /**
     * Triggers a token reset for the specified processor name.
     *
     * @param processorName the name of the event processor to reset
     * @return CompletableFuture that completes when the processor has been reset
     */
    public CompletableFuture<Void> resetProcessor(String processorName) {
        Assert.hasLength(processorName, "Processor name is mandatory and can't be empty!");

        // AF5: Use getComponents(EventProcessor.class) and find by name
        EventProcessor processor = configuration.getComponents(EventProcessor.class).get(processorName);

        if (processor == null) {
            logger.warn("No event processor found with name: {}", processorName);
            return CompletableFuture.completedFuture(null);
        }

        if (!(processor instanceof StreamingEventProcessor)) {
            logger.warn("Event processor {} is not a StreamingEventProcessor", processorName);
            return CompletableFuture.completedFuture(null);
        }

        return performReset((StreamingEventProcessor) processor);
    }

    /**
     * Performs the actual reset operation on a streaming event processor.
     *
     * <p>In AF5, PooledStreamingEventProcessor doesn't support the standard reset operation
     * (supportsReset() returns false). For these processors, we try to use resetTokens()
     * with a function that returns the latest token.</p>
     *
     * @param streamingEventProcessor the processor to reset
     * @return CompletableFuture that completes when the reset is done
     */
    private CompletableFuture<Void> performReset(StreamingEventProcessor streamingEventProcessor) {
        String processorName = streamingEventProcessor.name();
        boolean isPooled = streamingEventProcessor instanceof PooledStreamingEventProcessor;

        logger.info("Triggering token reset for event processor: {} (type: {})",
                processorName,
                isPooled ? "PooledStreamingEventProcessor" : "StreamingEventProcessor");

        // AF5: All lifecycle methods now return CompletableFuture
        // Chain: shutdown → resetTokens → start
        return streamingEventProcessor.shutdown()
                .thenCompose(v -> {
                    logger.info("Processor {} shut down successfully, resetting tokens to latest position", processorName);

                    // For PooledStreamingEventProcessor in AF5, we use resetTokens with a function
                    // that returns the latest token. This will throw ResetNotSupportedException
                    // because the feature is not yet implemented (TODO #3304)
                    if (isPooled) {
                        logger.info("Attempting reset for PooledStreamingEventProcessor: {}", processorName);
                        // AF5 API: resetTokens(source -> source.latestToken(null))
                        return streamingEventProcessor.resetTokens(source -> source.latestToken(null));
                    } else {
                        // For regular streaming processors, check if reset is supported first
                        if (!streamingEventProcessor.supportsReset()) {
                            logger.warn("Event processor {} does not support reset operations", processorName);
                            return CompletableFuture.completedFuture(null);
                        }
                        // AF5 API change: createTailToken() → latestToken(ProcessingContext)
                        return streamingEventProcessor.resetTokens(source -> source.latestToken(null));
                    }
                })
                .thenCompose(v -> {
                    logger.info("Tokens reset successfully for {}, restarting processor", processorName);
                    return streamingEventProcessor.start();
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Error during token reset for processor {}", processorName, throwable);
                    } else {
                        logger.info("Token reset completed successfully for processor: {}", processorName);
                    }
                });
    }
}
