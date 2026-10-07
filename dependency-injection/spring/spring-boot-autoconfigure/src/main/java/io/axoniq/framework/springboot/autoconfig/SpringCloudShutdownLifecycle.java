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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springcloud.query.IncomingQueryInvoker;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tells the subscribers of the subscription queries this member answers that it is leaving, before the web server
 * stops serving their streams.
 * <p>
 * Spring stops the web server, first draining and then cutting its open requests, before it reaches any of Axon
 * Framework's own shutdown phases. By the time the query connector would announce this member leaving, the streams
 * the announcement travels over are gone, and every subscriber sees a lost connection instead, which fails its
 * subscription. The draining itself waits on those streams too, delaying shutdown by the full graceful shutdown
 * timeout.
 * <p>
 * Running in a phase just above the web server's graceful shutdown, this lifecycle first stops the streaming event
 * processors, so that no update is emitted after the announcement, and then announces it through
 * {@link IncomingQueryInvoker#leave()}. Each subscriber then carries on with the members that remain, and the web
 * server has no subscription streams left to wait on. Axon Framework stops the same processors again in its own
 * phase, which does nothing for a processor that has already stopped.
 * <p>
 * Subscribing event processors are left to Axon Framework's own shutdown. They handle the events of commands this
 * member still accepts at this point, and stopping them early would leave those events unhandled.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
final class SpringCloudShutdownLifecycle implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudShutdownLifecycle.class);

    /**
     * The phase this lifecycle stops in: one above Spring Boot's {@code WebServerGracefulShutdownLifecycle}, so that
     * it stops before the web server starts draining its open requests.
     */
    static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 1024 + 1;

    // Matches Spring Boot's default timeout per shutdown phase, which bounds the asynchronous stop in the same way.
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectProvider<AxonConfiguration> configuration;
    private final IncomingQueryInvoker invoker;

    private volatile boolean running;

    /**
     * Constructs a {@code SpringCloudShutdownLifecycle} stopping the streaming event processors of the given
     * {@code configuration}, and announcing this member leaving through the given {@code invoker}.
     *
     * @param configuration provides the configuration holding the streaming event processors to stop
     * @param invoker       the invoker answering the subscriptions to announce this member leaving on
     */
    SpringCloudShutdownLifecycle(ObjectProvider<AxonConfiguration> configuration, IncomingQueryInvoker invoker) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null.");
        this.invoker = Objects.requireNonNull(invoker, "The invoker must not be null.");
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        leave().orTimeout(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();
    }

    @Override
    public void stop(Runnable callback) {
        leave().whenComplete((result, failure) -> callback.run());
    }

    private CompletableFuture<Void> leave() {
        return stopStreamingProcessors().handle((result, failure) -> {
            try {
                if (failure == null) {
                    invoker.leave();
                } else {
                    // A processor that did not stop may still emit updates, so announcing this member leaving would
                    // tell subscribers they missed nothing when they might. The web server cuts the streams instead,
                    // which subscribers rightly see as a lost connection.
                    logger.warn("Could not stop the streaming event processors before the web server shuts down. "
                                        + "The subscriptions this member answers will fail.", failure);
                }
            } finally {
                running = false;
            }
            return FutureUtils.ignoreResult(result);
        });
    }

    private CompletableFuture<Void> stopStreamingProcessors() {
        AxonConfiguration axonConfiguration = configuration.getIfAvailable();
        if (axonConfiguration == null) {
            return FutureUtils.emptyCompletedFuture();
        }
        try {
            return FutureUtils.allOrEmpty(axonConfiguration.getComponents(StreamingEventProcessor.class)
                                                           .values()
                                                           .stream()
                                                           .map(StreamingEventProcessor::shutdown)
                                                           .toList());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
