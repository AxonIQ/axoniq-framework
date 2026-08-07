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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Restarts the running {@link StreamingEventProcessor StreamingEventProcessors} whenever the set of tenants changes, so
 * a processor streaming across tenants re-opens its stream with the current tenants.
 * <p>
 * A running stream cannot change its set of tenants. The merged stream is assembled over the tenants present when it
 * opened, so a tenant added afterwards is not read and a removed tenant's stream stays open. Re-opening the stream with
 * the current tenants requires a processor restart, which the coordinator supports.
 * <p>
 * The restart follows the {@link MultiTenantEventStorageEngine} handed to
 * {@link #follow(MultiTenantEventStorageEngine)} at startup, rather than the
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantProvider TenantProvider} that engine itself follows. A
 * tenant change reaches the provider's subscribers in turn, and a subscriber ahead of the engine may take real time
 * over its own registration, such as one opening a connection for the tenant. A restart takes barely any time by
 * comparison, so a restart driven by the provider can re-open the stream while the engine still reports the previous
 * tenants. The new tenant is then missing from a stream that nothing re-opens again, because no further tenant change
 * follows. Following the engine rules that out: it announces a change only once that change is visible through its own
 * tenants.
 * <p>
 * Restarts are coalesced onto a single thread: a burst of tenant changes, such as the initial discovery of several
 * tenants, results in as few restarts as possible while still ending on the current tenant set. Every restart pauses
 * processing for all tenants briefly. Whether the tenants discovered at startup request a restart depends on whether
 * this restarter subscribed before they were registered, which is not fixed. Either way the processors start far later
 * in the lifecycle, so such a request finds nothing running and each processor opens its stream over the full startup
 * set of its own accord.
 * <p>
 * Every running streaming event processor is restarted, not only the ones consuming across tenants. With the module on
 * the classpath multi-tenancy is on by default, so the event store routes across tenants and a pooled streaming
 * processor consumes them all. Restarting a processor that reads another source is harmless. It re-opens its own
 * source. Restarting only the multi-tenant processors would require inspecting each processor's configured source,
 * which the processor API does not expose, and tenant changes are infrequent, so restarting all of them is the
 * simpler and safe choice.
 * <p>
 * Each processor's shutdown-and-start is bounded by a safety-net timeout, so a processor that never completes its
 * shutdown or start cannot block the restart thread. It is taken from the
 * {@link MultiTenantStreamingProcessorRestartConfiguration}, which defaults to 30 seconds. A deployment whose
 * processors are slow to stop and start can register a customized
 * {@link MultiTenantStreamingProcessorRestartConfiguration} to raise it.
 * <p>
 * Internal, because it is registered by the {@link MultiTenancyConfigurationDefaults} enhancer. The only member an
 * enhancer building a tenant-routing engine calls is {@link #follow(MultiTenantEventStorageEngine)}.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantStreamingProcessorRestarter implements DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantStreamingProcessorRestarter.class);

    private final Configuration configuration;
    private final Duration restartTimeout;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean restarting = new AtomicBoolean(false);
    private final AtomicBoolean restartRequested = new AtomicBoolean(false);
    private final AtomicLong restartCount = new AtomicLong();

    private final AtomicReference<@Nullable Followed> followed = new AtomicReference<>();
    @Nullable
    private volatile ExecutorService restartExecutor;

    /**
     * Constructs a restarter for the given {@code configuration}.
     *
     * @param configuration the configuration supplying the processors to restart and the restart timeout
     */
    MultiTenantStreamingProcessorRestarter(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
        this.restartTimeout =
                configuration.getComponent(MultiTenantStreamingProcessorRestartConfiguration.class).restartTimeout();
    }

    /**
     * Prepares the restart executor, so tenant changes from this point on trigger a restart of the running streaming
     * event processors.
     */
    void start() {
        // Create the executor before marking running, so a running restarter always has an executor to drain onto.
        restartExecutor = Executors.newSingleThreadExecutor(
                new AxonThreadFactory("MultiTenantStreamingProcessorRestarter"));
        running.set(true);
    }

    /**
     * Follows the tenant changes of the given {@code engine}, whose tenants decide what a re-opened stream spans.
     * <p>
     * Must be called with the engine an enhancer built, from a start handler on that engine's own component
     * definition, rather than with one resolved from the {@link Configuration}. The engine is registered under
     * {@link org.axonframework.eventsourcing.eventstore.EventStorageEngine EventStorageEngine} and any enhancer may
     * decorate that type, so a resolved instance can be a decorator, which announces no tenant change at all.
     * <p>
     * May be called before or after {@link #start()}, since a change announced while this restarter is not running is
     * ignored. Only one engine can be followed, since a second one's tenants decide nothing about the merged stream the
     * first one spans.
     *
     * @param engine the engine whose tenant changes re-open the streams of the running streaming event processors
     * @throws NullPointerException       if the given {@code engine} is {@code null}
     * @throws AxonConfigurationException if {@code this} restarter already follows an engine
     */
    @Internal
    public void follow(MultiTenantEventStorageEngine engine) {
        Objects.requireNonNull(engine, "The multi-tenant event storage engine must not be null");
        Registration engineSubscription = engine.subscribe(this::requestRestart);
        if (!followed.compareAndSet(null, new Followed(engine, engineSubscription))) {
            // Left subscribed, a second engine would restart on tenants that decide nothing about the merged stream.
            engineSubscription.cancel();
            throw new AxonConfigurationException(
                    "This restarter already follows a multi-tenant event storage engine, so it cannot follow another.");
        }
    }

    /**
     * Warns when nothing handed this restarter an engine to follow, so a tenant change reaches no streaming event
     * processor.
     * <p>
     * Called once every component that could hand over an engine has started, since a handover and this restarter's own
     * start share a lifecycle phase and therefore have no order between them.
     */
    void warnWhenFollowingNothing() {
        if (followed.get() == null) {
            logger.warn("""
                        No multi-tenant event storage engine follows the tenants of this application, so a tenant \
                        added or removed at runtime does not re-open the streams of the running streaming event \
                        processors. \
                        Events of such a tenant are then only picked up after a restart of the application.""");
        }
    }

    /**
     * Cancels the tenant subscription and shuts the restart executor down, so no further restarts are triggered.
     */
    void stop() {
        running.set(false);
        // Taken out in one step, so two concurrent stops cannot cancel the same subscription twice. A follow cannot
        // interleave: it runs in a start phase and this in a shutdown phase.
        Followed currentlyFollowed = followed.getAndSet(null);
        if (currentlyFollowed != null) {
            currentlyFollowed.registration().cancel();
        }
        ExecutorService executor = restartExecutor;
        if (executor != null) {
            executor.shutdownNow();
            restartExecutor = null;
        }
    }

    private void requestRestart() {
        if (!running.get()) {
            return;
        }
        restartRequested.set(true);
        drain();
    }

    private void drain() {
        if (!running.get()) {
            return;
        }
        if (!restarting.compareAndSet(false, true)) {
            return;
        }
        // Read into a local, since stop() may null the field after the running check above.
        ExecutorService executor = restartExecutor;
        if (executor == null) {
            restarting.set(false);
            return;
        }
        try {
            executor.execute(this::runRestartCycle);
        } catch (RejectedExecutionException rejected) {
            // stop() shut the executor down between the running check and this submission. Nothing left to do.
            restarting.set(false);
        }
    }

    private void runRestartCycle() {
        try {
            while (running.get() && restartRequested.compareAndSet(true, false)) {
                restartRunningProcessors();
            }
        } catch (Exception failure) {
            logger.warn("Error while restarting streaming event processors after a tenant change.", failure);
        } finally {
            restarting.set(false);
            // A request may have arrived after the last drain of restartRequested but before restarting was cleared.
            if (restartRequested.get()) {
                drain();
            }
        }
    }

    private void restartRunningProcessors() {
        restartCount.incrementAndGet();
        for (StreamingEventProcessor processor : configuration.getComponents(StreamingEventProcessor.class).values()) {
            // Re-check on every processor, so a stop() midway through the set does not start the remaining processors.
            if (!running.get()) {
                return;
            }
            if (processor.isRunning()) {
                restart(processor);
            }
        }
    }

    private void restart(StreamingEventProcessor processor) {
        logger.info("Restarting streaming event processor [{}] to apply the current tenant set.", processor.name());
        try {
            processor.shutdown()
                     // A stop() during the shutdown must not bring the processor back up, so the start is skipped once
                     // no longer running. The processor is left stopped, matching stop()'s intent.
                     .thenCompose(ignored -> running.get() ? processor.start() : FutureUtils.emptyCompletedFuture())
                     .orTimeout(restartTimeout.toMillis(), TimeUnit.MILLISECONDS)
                     .join();
        } catch (Exception failure) {
            // One processor failing or timing out must not skip the others, so it is caught per processor rather than
            // aborting the loop. The next tenant change restarts it again.
            logger.warn("""
                        Restarting streaming event processor [{}] failed. The other processors still restart, \
                        and the next tenant change retries this one.""",
                        processor.name(), failure);
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("running", running.get());
        Followed currentlyFollowed = followed.get();
        descriptor.describeProperty("followingTenantChanges", currentlyFollowed != null);
        if (currentlyFollowed != null) {
            // Only described when present, since a descriptor cannot hold a null value.
            descriptor.describeProperty("followedEngine", currentlyFollowed.engine());
        }
        descriptor.describeProperty("restartCount", restartCount.get());
        descriptor.describeProperty("restartTimeout", restartTimeout);
    }

    /**
     * The engine {@code this} restarter follows and the subscription it holds on that engine, so both are taken on and
     * given up in one step.
     *
     * @param engine       the engine whose tenant changes drive the restarts
     * @param registration the subscription held on that {@code engine}
     */
    private record Followed(MultiTenantEventStorageEngine engine, Registration registration) {

    }
}
