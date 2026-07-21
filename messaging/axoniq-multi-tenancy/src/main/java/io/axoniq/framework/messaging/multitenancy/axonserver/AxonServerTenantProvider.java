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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.configuration.MessagingConfigurationDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.axonserver.AxonServerTenantUtils.tenantDescriptor;
import static java.util.Objects.requireNonNull;

/**
 * Axon Server implementation of the {@link TenantProvider}. Uses Axon Server's multi-context support to construct
 * tenant-specific segments of all {@link MultiTenantAwareComponent MultiTenantAwareComponents}.
 * <p>
 * This provider can:
 * <ul>
 *     <li>Discover tenants from predefined context names</li>
 *     <li>Dynamically discover tenants via Axon Server's Admin API</li>
 *     <li>Subscribe to context updates to add/remove tenants at runtime</li>
 * </ul>
 * <p>
 * This class is Spring-agnostic and can be used with any framework that uses Axon Server.
 * Lifecycle management (start/shutdown) is handled through the configuration API via
 * {@link MessagingConfigurationDefaults}.
 *
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 * @see TenantProvider
 * @see TenantConnectPredicate
 * @see MessagingConfigurationDefaults
 * @since 4.6.0
 */
@Internal
public class AxonServerTenantProvider implements TenantProvider {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerTenantProvider.class);

    private final Set<TenantDescriptor> tenantDescriptors = ConcurrentHashMap.newKeySet();

    private final TenantConnectPredicate tenantConnectPredicate;
    private final AxonServerConnectionManager axonServerConnectionManager;

    // Guarded by 'this': every lifecycle transition mutates these under the monitor, so a registration is never added
    // in parallel with the cancellation that should cover it. One entry per tenant-component pair lets cancellation
    // select by tenant or by component. 'closed' blocks late registrations once shutdown cancelled everything.
    private final List<MultiTenantAwareComponent> tenantAwareComponents = new ArrayList<>();
    private final List<TenantRegistration> registrations = new ArrayList<>();
    private boolean closed = false;

    // Holds the active context-update stream once started. Initialised to a closed sentinel so shutdown() can
    // unconditionally close whatever is in the reference without a null-check, and subscribeToUpdates() can use
    // compareAndSet to avoid a race between concurrent start/shutdown calls.
    private final AtomicReference<ResultStream<ContextUpdate>> contextUpdatesStream =
            new AtomicReference<>(ClosedResultStream.instance());

    /**
     * Constructs an AxonServerTenantProvider with the given connection manager and tenant connect predicate.
     *
     * @param axonServerConnectionManager the connection manager for Axon Server
     * @param tenantConnectPredicate      the predicate to filter which contexts become tenants
     */
    public AxonServerTenantProvider(AxonServerConnectionManager axonServerConnectionManager,
                                    TenantConnectPredicate tenantConnectPredicate) {
        this.axonServerConnectionManager = requireNonNull(axonServerConnectionManager,
                                                          "AxonServerConnectionManager is required");
        this.tenantConnectPredicate = requireNonNull(tenantConnectPredicate,
                                                     "TenantConnectPredicate is required");
    }

    /**
     * Start this {@link TenantProvider}, by adding all tenants and subscribing to the
     * {@link AxonServerConnectionManager} for context updates.
     *
     * @return a {@link CompletableFuture} that completes when the provider has started
     */
    public CompletableFuture<Void> start() {
        return CompletableFuture.runAsync(() -> {
            getInitialTenants().forEach(this::addTenant);
            subscribeToUpdates();
        });
    }

    private List<TenantDescriptor> getInitialTenants() {
        List<TenantDescriptor> initialTenants = Collections.emptyList();
        try {
            List<ContextOverview> contexts = adminChannel()
                    .getAllContexts()
                    .orTimeout(30, TimeUnit.SECONDS)
                    .join();

            initialTenants = contexts.stream()
                                     .map(AxonServerTenantUtils::tenantDescriptor)
                                     .filter(tenantConnectPredicate)
                                     .toList();
        } catch (Exception e) {
            logger.error("Error while getting initial tenants", e);
        }
        return initialTenants;
    }

    private void subscribeToUpdates() {
        try {
            ResultStream<ContextUpdate> stream = adminChannel().subscribeToContextUpdates();
            contextUpdatesStream.set(stream);
            stream.onAvailable(() -> {
                try {
                    // nextIfAvailable() can return null despite its non-null annotation, so guard against it.
                    ContextUpdate contextUpdate = stream.nextIfAvailable();
                    if (contextUpdate == null) {
                        return;
                    }
                    switch (contextUpdate.getType()) {
                        case CREATED:
                            handleContextCreated(contextUpdate);
                            break;
                        case DELETED:
                            removeTenant(TenantDescriptor.tenantWithId(contextUpdate.getContext()));
                            break;
                        default:
                            // Ignore other update types
                            break;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("Error while subscribing to context updates", e);
        }
    }

    private void handleContextCreated(ContextUpdate contextUpdate) {
        try {
            TenantDescriptor newTenant =
                    tenantDescriptor(adminChannel()
                                             .getContextOverview(contextUpdate.getContext())
                                             .orTimeout(30, TimeUnit.SECONDS)
                                             .join());
            if (tenantConnectPredicate.test(newTenant)) {
                addTenant(newTenant);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(tenantDescriptors);
    }

    /**
     * Adds the given {@code tenantDescriptor} as a known tenant, registering and starting every subscribed
     * {@link MultiTenantAwareComponent} for it. A tenant that is already known is ignored, so its components are never
     * registered twice.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} representing the tenant to be added
     */
    public synchronized void addTenant(TenantDescriptor tenantDescriptor) {
        // Skip when shutting down, or when the tenant is already known, so a component is never registered twice.
        if (closed || !tenantDescriptors.add(tenantDescriptor)) {
            return;
        }
        tenantAwareComponents.forEach(component -> registrations.add(new TenantRegistration(
                tenantDescriptor, component, component.registerAndStartTenant(tenantDescriptor)
        )));
    }

    /**
     * Removes a tenant from the system.
     * <p>
     * This method checks if the provided {@link TenantDescriptor} is in the set of known tenants. If it is, the method
     * removes the tenant and cancels all its registrations.
     * {@link MultiTenantAwareComponent MultiTenantAwareComponents} are then unregistered in reverse order of their
     * registration. It then disconnects the tenant from the Axon Server.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} representing the tenant to be removed
     */
    public void removeTenant(TenantDescriptor tenantDescriptor) {
        // The disconnect is kept out of the monitor, so no lifecycle transition blocks on the network call.
        if (deregisterTenant(tenantDescriptor)) {
            axonServerConnectionManager.disconnect(tenantDescriptor.tenantId());
        }
    }

    private synchronized boolean deregisterTenant(TenantDescriptor tenantDescriptor) {
        if (!tenantDescriptors.remove(tenantDescriptor)) {
            return false;
        }
        cancelRegistrationsMatching(registration -> registration.tenant().equals(tenantDescriptor));
        return true;
    }

    @Override
    public synchronized Registration subscribe(MultiTenantAwareComponent component) {
        tenantAwareComponents.add(component);
        tenantDescriptors.forEach(tenantDescriptor -> registrations.add(new TenantRegistration(
                tenantDescriptor, component, component.registerTenant(tenantDescriptor)
        )));

        // Cancelling covers every registration made on the component's behalf, including tenants added after
        // subscribing, as the TenantProvider#subscribe contract requires.
        return () -> unsubscribe(component);
    }

    private synchronized boolean unsubscribe(MultiTenantAwareComponent component) {
        tenantAwareComponents.remove(component);
        cancelRegistrationsMatching(registration -> registration.component() == component);
        return true;
    }

    // Cancels in reverse registration order, so the last registered component is deregistered first. A failing
    // cancellation is logged and skipped, so the remaining registrations are still cancelled.
    private synchronized void cancelRegistrationsMatching(Predicate<TenantRegistration> criterion) {
        List<TenantRegistration> matching = registrations.stream().filter(criterion).toList();
        registrations.removeAll(matching);
        for (TenantRegistration tenantRegistration : matching.reversed()) {
            try {
                tenantRegistration.registration().cancel();
            } catch (Exception e) {
                logger.warn("Error while cancelling the tenant [{}] registration of component [{}].",
                            tenantRegistration.tenant().tenantId(), tenantRegistration.component(), e);
            }
        }
    }

    /**
     * Shuts down the AxonServerTenantProvider by deregistering all subscribed components.
     * <p>
     * Every tenant registration of every subscribed component is cancelled in reverse registration order, so
     * last-registered components are deregistered first and get the opportunity to perform any necessary cleanup.
     *
     * @return a {@link CompletableFuture} that completes when the provider has shut down
     */
    public CompletableFuture<Void> shutdown() {
        contextUpdatesStream.getAndSet(ClosedResultStream.instance()).close();
        return CompletableFuture.runAsync(this::cancelAllRegistrations);
    }

    private synchronized void cancelAllRegistrations() {
        // Marks the provider closed before cancelling, so a context update still in flight skips registration in
        // addTenant rather than adding an entry that would outlive this cancellation.
        closed = true;
        cancelRegistrationsMatching(registration -> true);
    }

    private AdminChannel adminChannel() {
        return axonServerConnectionManager.getConnection(ADMIN_CONTEXT).adminChannel();
    }

    private record TenantRegistration(TenantDescriptor tenant,
                                      MultiTenantAwareComponent component,
                                      Registration registration) {

    }

    /**
     * A no-op {@link ResultStream} used as the initial and post-shutdown sentinel in {@link #contextUpdatesStream}, so
     * the reference is never {@code null}.
     */
    private static final class ClosedResultStream implements ResultStream<ContextUpdate> {

        private static final ClosedResultStream INSTANCE = new ClosedResultStream();

        static ClosedResultStream instance() {
            return INSTANCE;
        }

        private ClosedResultStream() {
        }

        @Override
        public ContextUpdate peek() {
            return null;
        }

        @Override
        public ContextUpdate nextIfAvailable() {
            return null;
        }

        @Override
        public ContextUpdate nextIfAvailable(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public ContextUpdate next() {
            return null;
        }

        @Override
        public void onAvailable(Runnable callback) {
            // this sentinel never has any updates, so the callback is never invoked
        }

        @Override
        public void close() {
            // this sentinel is already closed
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public Optional<Throwable> getError() {
            return Optional.empty();
        }
    }
}
