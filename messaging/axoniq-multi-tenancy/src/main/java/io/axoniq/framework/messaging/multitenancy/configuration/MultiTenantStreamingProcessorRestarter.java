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

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
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

/**
 * Restarts the running {@link StreamingEventProcessor StreamingEventProcessors} whenever the set of tenants changes, so
 * a processor streaming across tenants re-opens its stream with the current tenants.
 * <p>
 * A running stream cannot change its set of tenants. The merged stream is assembled over the tenants present when it
 * opened, so a tenant added afterwards is not read and a removed tenant's stream stays open. Re-opening the stream with
 * the current tenants requires a processor restart, which the coordinator supports. This component subscribes itself to
 * the {@link TenantProvider} at startup, so it is notified of tenants discovered at startup and of tenants added or
 * removed at runtime, and requests a restart on each.
 * <p>
 * Restarts are coalesced onto a single thread: a burst of tenant changes, such as the initial discovery of several
 * tenants, results in as few restarts as possible while still ending on the current tenant set. Every restart pauses
 * processing for all tenants briefly. Both {@link #registerTenant(TenantDescriptor)} and
 * {@link #registerAndStartTenant(TenantDescriptor)} request a restart, because the {@code TenantProvider} starts after
 * the processors, so the initial discovery arrives while the processors are already running.
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
 * {@link MultiTenantProcessorRestartConfiguration}, which defaults to 30 seconds. A deployment whose processors are
 * slow to stop and start can register a customized {@link MultiTenantProcessorRestartConfiguration} to raise it.
 * <p>
 * Internal, because it is registered by the {@link MultiTenancyConfigurationDefaults} enhancer and never used directly.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
class MultiTenantStreamingProcessorRestarter implements MultiTenantAwareComponent {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantStreamingProcessorRestarter.class);

    private final Configuration configuration;
    private final Duration restartTimeout;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean restarting = new AtomicBoolean(false);
    private final AtomicBoolean restartRequested = new AtomicBoolean(false);
    private final AtomicLong restartCount = new AtomicLong();

    @Nullable
    private volatile Registration subscription;
    @Nullable
    private volatile ExecutorService restartExecutor;

    /**
     * Constructs a restarter for the given {@code configuration}.
     *
     * @param configuration the configuration supplying the tenant provider and the streaming event processors
     */
    MultiTenantStreamingProcessorRestarter(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
        this.restartTimeout =
                configuration.getComponent(MultiTenantProcessorRestartConfiguration.class).restartTimeout();
    }

    /**
     * Subscribes this restarter to the {@link TenantProvider} and prepares the restart executor, so tenant changes from
     * this point on trigger a restart of the running streaming event processors.
     */
    void start() {
        // Create the executor before marking running, so a running restarter always has an executor to drain onto.
        restartExecutor = Executors.newSingleThreadExecutor(
                new AxonThreadFactory("MultiTenantStreamingProcessorRestarter"));
        running.set(true);
        subscription = configuration.getComponent(TenantProvider.class).subscribe(this);
    }

    /**
     * Cancels the tenant subscription and shuts the restart executor down, so no further restarts are triggered.
     */
    void stop() {
        running.set(false);
        Registration currentSubscription = subscription;
        if (currentSubscription != null) {
            currentSubscription.cancel();
            subscription = null;
        }
        ExecutorService executor = restartExecutor;
        if (executor != null) {
            executor.shutdownNow();
            restartExecutor = null;
        }
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        requestRestart();
        return () -> {
            requestRestart();
            return true;
        };
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
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
        descriptor.describeProperty("restartCount", restartCount.get());
        descriptor.describeProperty("restartTimeout", restartTimeout);
    }
}
