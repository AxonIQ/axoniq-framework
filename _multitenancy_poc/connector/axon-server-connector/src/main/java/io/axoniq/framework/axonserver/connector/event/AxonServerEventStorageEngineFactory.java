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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.configuration.Component;
import org.axonframework.common.configuration.ComponentFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.InstantiatedComponentDefinition;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Optional;

/**
 * A {@link ComponentFactory} implementation that generates {@link AxonServerEventStorageEngine} instances.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class AxonServerEventStorageEngineFactory implements ComponentFactory<AxonServerEventStorageEngine> {

    /**
     * The expected prefix for <b>any</b> {@code name} given when {@link #construct(String, Configuration) constructing}
     * an instance. An {@link Optional#empty() empty Optional} will be returned if the {@code name} does not start with
     * {@code "storageEngine"}.
     */
    public static final String ENGINE_PREFIX = "storageEngine";

    /**
     * The {@code name} delimiter used when deriving the context name for the {@link AxonServerEventStorageEngine} under
     * construction. If the {@code name} when {@link #construct(String, Configuration) constructing} an instance does
     * not contain this delimiter, an {@link Optional#empty() empty Optional} will be returned.
     */
    public static final String CONTEXT_DELIMITER = "@";

    /**
     * Constructs an {@link AxonServerEventStorageEngine} for the given {@code context}, retrieving a
     * {@link AxonServerConnectionManager} and {@link EventConverter} from the given {@code config}.
     * <p>
     * The {@code context} is used to request an {@link AxonServerConnection} from the
     * {@code AxonServerConnectionManager}.
     *
     * @param context the name of the context for which to open an {@link AxonServerConnection} for the
     *                {@link AxonServerEventStorageEngine} under construction
     * @param config  the configuration from which to retrieve an {@link AxonServerConnectionManager},
     *                {@link EventConverter}, and optional {@link EventTypeResolver} for the {@link AxonServerEventStorageEngine} under construction
     * @return an {@link AxonServerEventStorageEngine}, connecting to the given {@code context}
     */
    public static AxonServerEventStorageEngine constructForContext(String context,
                                                                            Configuration config) {
        AxonServerConnection connection = config.getComponent(AxonServerConnectionManager.class)
                                                .getConnection(context);
        EventConverter eventConverter = config.getComponent(EventConverter.class);
        EventTypeResolver eventTypeResolver = config.getOptionalComponent(EventTypeResolver.class)
                                                    .orElse(EventTypeResolver.DEFAULT);
        return new AxonServerEventStorageEngine(connection, eventConverter, eventTypeResolver);
    }

    @Override
    public Class<AxonServerEventStorageEngine> forType() {
        return AxonServerEventStorageEngine.class;
    }

    @Override
    public Optional<Component<AxonServerEventStorageEngine>> construct(String name,
                                                                       Configuration config) {
        return contextNameFrom(name).map(context -> constructForContext(context, config))
                                    .map(engine -> new InstantiatedComponentDefinition<>(
                                            new Component.Identifier<>(forType(), name),
                                            engine
                                    ));
    }

    @Override
    public void registerShutdownHandlers(LifecycleRegistry registry) {
        // Nothing to do here
    }

    private static Optional<String> contextNameFrom(String name) {
        return name.startsWith(ENGINE_PREFIX + CONTEXT_DELIMITER)
                ? Optional.of(name.substring(name.indexOf(CONTEXT_DELIMITER)))
                : Optional.empty();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("type", forType());
        descriptor.describeProperty("nameFormat", ENGINE_PREFIX + CONTEXT_DELIMITER + "{context-name}");
    }
}
