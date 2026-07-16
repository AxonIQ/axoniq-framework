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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.Registration;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.configuration.MessagingConfigurationDefaults;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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
 * @since 5.3.0
 */
@Internal
public class AxonServerTenantProvider implements TenantProvider {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerTenantProvider.class);
    private static final String ADMIN_CTX = "_admin";

    private final List<MultiTenantAwareComponent> tenantAwareComponents = new CopyOnWriteArrayList<>();
    private final Set<TenantDescriptor> tenantDescriptors = ConcurrentHashMap.newKeySet();

    @Nullable
    private final String preDefinedContexts;
    private final TenantConnectPredicate tenantConnectPredicate;
    private final AxonServerConnectionManager axonServerConnectionManager;

    private final ConcurrentHashMap<TenantDescriptor, List<Registration>> registrationMap = new ConcurrentHashMap<>();

    /**
     * Constructs an AxonServerTenantProvider with the given connection manager and tenant connect predicate.
     *
     * @param axonServerConnectionManager the connection manager for Axon Server
     * @param tenantConnectPredicate      the predicate to filter which contexts become tenants
     */
    public AxonServerTenantProvider(AxonServerConnectionManager axonServerConnectionManager,
                                    TenantConnectPredicate tenantConnectPredicate) {
        this(axonServerConnectionManager, tenantConnectPredicate, null);
    }

    /**
     * Constructs an AxonServerTenantProvider with the given connection manager, tenant connect predicate, and
     * optional predefined contexts.
     *
     * @param axonServerConnectionManager the connection manager for Axon Server
     * @param tenantConnectPredicate      the predicate to filter which contexts become tenants
     * @param preDefinedContexts          comma-separated list of context names to use instead of discovering from Axon
     *                                    Server's Admin API, or {@code null} for auto-discovery
     */
    public AxonServerTenantProvider(AxonServerConnectionManager axonServerConnectionManager,
                                    TenantConnectPredicate tenantConnectPredicate,
                                    @Nullable String preDefinedContexts) {
        this.axonServerConnectionManager = requireNonNull(axonServerConnectionManager,
                                                          "AxonServerConnectionManager is required");
        this.tenantConnectPredicate = requireNonNull(tenantConnectPredicate,
                                                     "TenantConnectPredicate is required");
        this.preDefinedContexts = preDefinedContexts;
    }

    /**
     * Start this {@link TenantProvider}, by adding all tenants and subscribing to the
     * {@link AxonServerConnectionManager} for context updates.
     *
     * @return a {@link CompletableFuture} that completes when the provider has started
     */
    public CompletableFuture<Void> start() {
        return CompletableFuture.runAsync(() -> {
            tenantDescriptors.addAll(getInitialTenants());
            tenantDescriptors.forEach(this::addTenant);
            if (preDefinedContexts == null || preDefinedContexts.isEmpty()) {
                subscribeToUpdates();
            }
        });
    }

    private List<TenantDescriptor> getInitialTenants() {
        List<TenantDescriptor> initialTenants = Collections.emptyList();
        try {
            // TODO: never tried predefined contexts with this new module, needs verification
            if (StringUtils.nonEmptyOrNull(preDefinedContexts)) {
                initialTenants = Arrays.stream(preDefinedContexts.split(","))
                                       .map(String::trim)
                                       .map(TenantDescriptor::tenantWithId)
                                       .toList();
            } else {
                initialTenants = getTenantsAPI();
            }
        } catch (Exception e) {
            logger.error("Error while getting initial tenants", e);
        }
        return initialTenants;
    }

    private void subscribeToUpdates() {
        try (ResultStream<ContextUpdate> contextUpdatesStream = axonServerConnectionManager.getConnection(ADMIN_CTX)
                                                                                           .adminChannel()
                                                                                           .subscribeToContextUpdates();
        ) {
            contextUpdatesStream.onAvailable(() -> {
                try {
                    ContextUpdate contextUpdate = contextUpdatesStream.nextIfAvailable();
                    // FIXME: failed also markd NonNull: java.lang.NullPointerException: Cannot invoke "io.axoniq.axonserver.grpc.admin.ContextUpdate.getType()" because "contextUpdate" is null
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
                    toTenantDescriptor(axonServerConnectionManager.getConnection(ADMIN_CTX)
                                                                  .adminChannel()
                                                                  .getContextOverview(contextUpdate.getContext())
                                                                  .orTimeout(30, TimeUnit.SECONDS)
                                                                  .join());
            if (tenantConnectPredicate.test(newTenant) && !tenantDescriptors.contains(newTenant)) {
                addTenant(newTenant);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public List<TenantDescriptor> getTenants() {
        return new ArrayList<>(tenantDescriptors);
    }

    private List<TenantDescriptor> getTenantsAPI() {
        return axonServerConnectionManager.getConnection(ADMIN_CTX)
                                          .adminChannel()
                                          .getAllContexts()
                                          .orTimeout(30, TimeUnit.SECONDS)
                                          .join()
                                          .stream()
                                          .map(this::toTenantDescriptor)
                                          .filter(tenantConnectPredicate)
                                          .toList();
    }

    private TenantDescriptor toTenantDescriptor(ContextOverview context) {
        Map<String, String> metaDataMap = new HashMap<>(context.getMetaDataMap());
        metaDataMap.putIfAbsent("replicationGroup", context.getReplicationGroup().getName());

        return new TenantDescriptor(context.getName(), metaDataMap);
    }

    /**
     * Adds a new tenant to the system.
     * <p>
     * This method adds the provided {@link TenantDescriptor} to the set of known tenants. Once added all
     * {@link MultiTenantAwareComponent MultiTenantAwareComponents} are registered and started for the new tenant.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} representing the tenant to be added
     */
    public void addTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.add(tenantDescriptor);
        tenantAwareComponents
                .forEach(component -> registrationMap
                        .computeIfAbsent(tenantDescriptor, t -> new CopyOnWriteArrayList<>())
                        .add(component.registerAndStartTenant(tenantDescriptor)));
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
        if (tenantDescriptors.remove(tenantDescriptor)) {
            List<Registration> registrations = registrationMap.remove(tenantDescriptor);
            if (registrations != null && !registrations.isEmpty()) {
                registrations.forEach(Registration::cancel);
            }
            axonServerConnectionManager.disconnect(tenantDescriptor.tenantId());
        }
    }

    @Override
    public Registration subscribe(MultiTenantAwareComponent component) {
        tenantAwareComponents.add(component);

        List<Registration> componentRegistrations = new CopyOnWriteArrayList<>();
        tenantDescriptors
                .forEach(tenantDescriptor -> {
                    Registration registration = component.registerTenant(tenantDescriptor);
                    registrationMap
                            .computeIfAbsent(tenantDescriptor, t -> new CopyOnWriteArrayList<>())
                            .add(registration);
                    componentRegistrations.add(registration);
                });

        return () -> {
            tenantAwareComponents.remove(component);
            componentRegistrations.forEach(Registration::cancel);
            registrationMap.values().forEach(list -> list.removeAll(componentRegistrations));
            return true;
        };
    }

    /**
     * Shuts down the AxonServerTenantProvider by deregistering all subscribed components.
     * <p>
     * The shutdown process involves the following steps:
     * <ol>
     *     <li>Iterates through all registered components for each tenant.</li>
     *     <li>Reverses the order of registrations for each tenant to ensure
     *         last-registered components are deregistered first.</li>
     *     <li>Invokes the cancel method on each registration, effectively
     *         deregistering the component from the tenant.</li>
     * </ol>
     * <p>
     * This method ensures that all resources associated with tenant management are properly
     * released and that components are given the opportunity to perform any necessary cleanup
     * in the reverse order of their registration.
     *
     * @return a {@link CompletableFuture} that completes when the provider has shut down
     */
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.runAsync(() -> registrationMap
                .values()
                .forEach(it -> it.reversed().forEach(Registration::cancel)
                )
        );
    }
}
