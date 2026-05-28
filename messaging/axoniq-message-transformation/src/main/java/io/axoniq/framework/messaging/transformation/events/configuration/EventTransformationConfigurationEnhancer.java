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

package io.axoniq.framework.messaging.transformation.events.configuration;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.TransformingEventStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link TransformingEventStore} decorator on the application's {@link EventStore}. Reads
 * the user-supplied {@link EventTransformerChain}, the active {@link MessageConverter}, and
 * the active {@link MessageTypeResolver} from the {@code Configuration} at
 * decorator-registration time; when no chain is registered the decorator is a no-op
 * pass-through.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class EventTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * Instantiated by the {@link java.util.ServiceLoader}; not intended for direct use.
     */
    public EventTransformationConfigurationEnhancer() {
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                EventStore.class,
                TransformingEventStore.DECORATION_ORDER,
                (config, name, delegate) -> config.getOptionalComponent(EventTransformerChain.class)
                                                  .<EventStore>map(chain -> new TransformingEventStore(
                                                          delegate,
                                                          chain,
                                                          config.getComponent(MessageConverter.class),
                                                          config.getComponent(MessageTypeResolver.class)))
                                                  .orElse(delegate));
    }
}
