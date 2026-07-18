/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.axoniq.framework.messaging.multitenancy.eventhandling.processing;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.EventTrackerStatus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.TrackingTokenSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * Tenant aware implementation of {@link StreamingEventProcessor} that encapsulates per-tenant
 * {@link StreamingEventProcessor}s and forwards corresponding actions to tenant-specific segments.
 * <p>
 * This processor acts as a decorator in the event processor decoration chain. When registered via
 * {@link MultiTenancyConfigurationDefaults},
 * it wraps the standard processor and creates per-tenant streaming processors that each process
 * events from their own tenant-specific event store.
 * <p>
 * Segment-level operations ({@link #splitSegment(int)}, {@link #mergeSegment(int)},
 * {@link #releaseSegment(int)}, {@link #resetTokens()}) are delegated to all tenant segments.
 * Status operations ({@link #processingStatus()}, {@link #maxCapacity()}) are aggregated
 * across all tenant segments.
 *
 * @author Jan Galinski
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 * @since 5.3.0
 */
public class MultiTenantEventProcessor implements StreamingEventProcessor, MultiTenantAwareComponent {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantEventProcessor.class);

    /**
     * The decoration order for the multi-tenant event processor decorator.
     * <p>
     * Uses {@code Integer.MIN_VALUE + 50} to ensure multi-tenant decoration runs early in the chain,
     * consistent with other multi-tenant decorators ({@code TenantRoutingCommandBus},
     * {@code TenantRoutingQueryBus}).
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 50;

    private final Map<TenantDescriptor, EventProcessor> tenantEventProcessorsSegments = new ConcurrentHashMap<>();
    private final String name;
    private final TenantEventProcessorSegmentFactory tenantEventProcessorSegmentFactory;

    private volatile boolean started = false;

    /**
     * Constructs a MultiTenantEventProcessor with the given {@code name} and
     * {@code tenantSegmentFactory}.
     *
     * @param name                 the name of this event processor
     * @param tenantSegmentFactory the factory used to construct tenant-specific {@link EventProcessor} segments
     */
    public MultiTenantEventProcessor(String name,
                                     TenantEventProcessorSegmentFactory tenantSegmentFactory) {
        this.name = Objects.requireNonNull(name, "The name is a hard requirement and should be provided");
        this.tenantEventProcessorSegmentFactory = Objects.requireNonNull(
                tenantSegmentFactory, "The TenantEventProcessorSegmentFactory is a hard requirement and should be provided"
        );
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Returns the tenant segments managed by this {@code MultiTenantEventProcessor}.
     *
     * @return a map of {@link TenantDescriptor} to {@link EventProcessor} representing tenant segments
     */
    public Map<TenantDescriptor, EventProcessor> tenantSegments() {
        return Collections.unmodifiableMap(tenantEventProcessorsSegments);
    }

    @Override
    public CompletableFuture<Void> start() {
        started = true;
        CompletableFuture<?>[] futures = tenantEventProcessorsSegments.values()
                .stream()
                .map(EventProcessor::start)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        started = false;
        CompletableFuture<?>[] futures = tenantEventProcessorsSegments.values()
                .stream()
                .map(EventProcessor::shutdown)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    /**
     * Indicates whether the {@link EventProcessor} for the given {@code tenantDescriptor} is currently running (i.e.
     * consuming events from its message source).
     *
     * @param tenantDescriptor the tenant descriptor referring to the {@link EventProcessor} for which to check if it is
     *                         currently running.
     * @return {@code true} when running, otherwise {@code false}
     */
    public boolean isRunning(TenantDescriptor tenantDescriptor) {
        EventProcessor processor = tenantEventProcessorsSegments.get(tenantDescriptor);
        if (processor == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return processor.isRunning();
    }

    /**
     * Indicates whether any tenant processor has been shut down due to an error.
     * <p>
     * Use {@link #isError(TenantDescriptor)} to check a specific tenant.
     *
     * @return {@code true} when any tenant processor is in error, otherwise {@code false}
     */
    @Override
    public boolean isError() {
        return tenantEventProcessorsSegments.values().stream().anyMatch(EventProcessor::isError);
    }

    /**
     * Indicates whether the {@link EventProcessor} for the given {@code tenantDescriptor} has been shut down due to an
     * error. In such case, the processor has forcefully shut down, as it wasn't able to automatically recover.
     * <p>
     * Note that this method returns {@code false} when the tenant processor was stopped using {@link #shutdown()}.
     *
     * @return {@code true} when paused due to an error, otherwise {@code false}
     */
    public boolean isError(TenantDescriptor tenantDescriptor) {
        EventProcessor processor = tenantEventProcessorsSegments.get(tenantDescriptor);
        if (processor == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return processor.isError();
    }

    // ---- StreamingEventProcessor methods ----

    @Override
    public String getTokenStoreIdentifier() {
        return name + "[multi-tenant]";
    }

    @Override
    public CompletableFuture<Void> releaseSegment(int segmentId) {
        return delegateToAllStreamingTenants(sep -> sep.releaseSegment(segmentId));
    }

    @Override
    public CompletableFuture<Void> releaseSegment(int segmentId, long releaseDuration, TimeUnit unit) {
        return delegateToAllStreamingTenants(sep -> sep.releaseSegment(segmentId, releaseDuration, unit));
    }

    @Override
    public CompletableFuture<Boolean> splitSegment(int segmentId) {
        return delegateToAllStreamingTenantsBoolean(sep -> sep.splitSegment(segmentId));
    }

    @Override
    public CompletableFuture<Boolean> mergeSegment(int segmentId) {
        return delegateToAllStreamingTenantsBoolean(sep -> sep.mergeSegment(segmentId));
    }

    @Override
    public boolean supportsReset() {
        return streamingTenantSegments().stream().allMatch(StreamingEventProcessor::supportsReset);
    }

    @Override
    public CompletableFuture<Void> resetTokens() {
        return delegateToAllStreamingTenants(StreamingEventProcessor::resetTokens);
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(@Nullable R resetContext) {
        return delegateToAllStreamingTenants(sep -> sep.resetTokens(resetContext));
    }

    @Override
    public CompletableFuture<Void> resetTokens(
            Function<TrackingTokenSource, CompletableFuture<TrackingToken>> initialTrackingTokenSupplier
    ) {
        return delegateToAllStreamingTenants(sep -> sep.resetTokens(initialTrackingTokenSupplier));
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(
            Function<TrackingTokenSource, CompletableFuture<TrackingToken>> initialTrackingTokenSupplier,
            @Nullable R resetContext
    ) {
        return delegateToAllStreamingTenants(sep -> sep.resetTokens(initialTrackingTokenSupplier, resetContext));
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(TrackingToken startPosition, @Nullable R resetContext) {
        return delegateToAllStreamingTenants(sep -> sep.resetTokens(startPosition, resetContext));
    }

    @Override
    public int maxCapacity() {
        return streamingTenantSegments().stream()
                .mapToInt(StreamingEventProcessor::maxCapacity)
                .sum();
    }

    @Override
    public Map<Integer, EventTrackerStatus> processingStatus() {
        Map<Integer, EventTrackerStatus> aggregated = new HashMap<>();
        for (StreamingEventProcessor segment : streamingTenantSegments()) {
            aggregated.putAll(segment.processingStatus());
        }
        return Collections.unmodifiableMap(aggregated);
    }

    // ---- Tenant management ----

    /**
     * {@inheritDoc}
     * <p>
     * Tenants can be only registered prior to {@link #start() starting} this processor. To register and start a tenant
     * during runtime, use {@link #registerAndStartTenant(TenantDescriptor)}
     */
    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        if (started) {
            throw new IllegalStateException("Cannot register tenant after processor has been started");
        }
        EventProcessor tenantSegment = tenantEventProcessorSegmentFactory.apply(tenantDescriptor);
        tenantEventProcessorsSegments.putIfAbsent(tenantDescriptor, tenantSegment);

        return () -> stopAndRemoveTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantEventProcessorsSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            EventProcessor tenantSegment = tenantEventProcessorSegmentFactory.apply(tenant);
            tenantSegment.start();
            return tenantSegment;
        });

        return () -> stopAndRemoveTenant(tenantDescriptor);
    }


    /**
     * Stops the given {@code tenant} and removes it from this processor. Note that this does not remove any potentially
     * persisted {@link TrackingToken TrackingTokens} from
     * {@link StreamingEventProcessor} instances!
     *
     * @param tenantDescriptor the tenant to stop and remove from this processor
     * @return a {@code boolean} indicating whether the tenant was removed
     */
    public boolean stopAndRemoveTenant(TenantDescriptor tenantDescriptor) {
        EventProcessor delegate = tenantEventProcessorsSegments.remove(tenantDescriptor);
        if (delegate != null) {
            delegate.shutdown();
            return true;
        }
        return false;
    }

    /**
     * Returns a list of all {@link EventProcessor} this instance manages.
     *
     * @return a list of all {@link EventProcessor} this instance manages
     */
    public List<EventProcessor> tenantEventProcessors() {
        return Collections.unmodifiableList(new ArrayList<>(tenantEventProcessorsSegments.values()));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
        descriptor.describeProperty("tenantSegments", tenantEventProcessorsSegments);
    }

    // ---- Internal helpers ----

    private List<StreamingEventProcessor> streamingTenantSegments() {
        return tenantEventProcessorsSegments.values()
                .stream()
                .filter(StreamingEventProcessor.class::isInstance)
                .map(StreamingEventProcessor.class::cast)
                .toList();
    }

    private CompletableFuture<Void> delegateToAllStreamingTenants(
            Function<StreamingEventProcessor, CompletableFuture<?>> action
    ) {
        CompletableFuture<?>[] futures = streamingTenantSegments()
                .stream()
                .map(action)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Boolean> delegateToAllStreamingTenantsBoolean(
            Function<StreamingEventProcessor, CompletableFuture<Boolean>> action
    ) {
        List<CompletableFuture<Boolean>> futures = streamingTenantSegments()
                .stream()
                .map(action)
                .toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> futures.stream().allMatch(f -> f.join()));
    }

}
