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

package io.axoniq.framework.kafka.configuration;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.TransformingStreamableEventSource;
import io.axoniq.framework.messaging.transformation.events.TransformingSubscribableEventSource;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;

/**
 * Applies the application's {@link EventTransformerChain}, when it has one, to the events Kafka sources read, so that
 * events from Kafka are transformed just like events read from the event store.
 * <p>
 * The {@code axoniq-message-transformation} module is optional. This class only touches its types once it is known to
 * be on the classpath.
 * <p>
 * Internal, because the {@link KafkaConnector} uses it while building sources.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class EventTransformationSupport {

    private static final boolean TRANSFORMATION_AVAILABLE = isAvailable();

    private EventTransformationSupport() {
        // utility class
    }

    private static boolean isAvailable() {
        try {
            Class.forName("io.axoniq.framework.messaging.transformation.events.EventTransformerChain",
                          false,
                          EventTransformationSupport.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /**
     * Returns the given {@code source}, transforming its events when the configuration holds an
     * {@link EventTransformerChain}.
     *
     * @param source        the source to transform the events of
     * @param configuration the application's configuration
     * @return the source as processors should see it
     */
    static StreamableEventSource transforming(StreamableEventSource source, Configuration configuration) {
        return TRANSFORMATION_AVAILABLE ? Transformer.transforming(source, configuration) : source;
    }

    /**
     * Returns the given {@code source}, transforming its events when the configuration holds an
     * {@link EventTransformerChain}.
     *
     * @param source        the source to transform the events of
     * @param configuration the application's configuration
     * @return the source as processors should see it
     */
    static SubscribableEventSource transforming(SubscribableEventSource source, Configuration configuration) {
        return TRANSFORMATION_AVAILABLE ? Transformer.transforming(source, configuration) : source;
    }

    /**
     * Holds the references to the optional module, so they are only resolved once the module is known to be present.
     */
    private static final class Transformer {

        private static StreamableEventSource transforming(StreamableEventSource source, Configuration config) {
            return config.getOptionalComponent(EventTransformerChain.class)
                         .<StreamableEventSource>map(chain -> new TransformingStreamableEventSource(
                                 source, chain,
                                 config.getComponent(MessageConverter.class),
                                 config.getComponent(MessageTypeResolver.class)
                         ))
                         .orElse(source);
        }

        private static SubscribableEventSource transforming(SubscribableEventSource source, Configuration config) {
            return config.getOptionalComponent(EventTransformerChain.class)
                         .<SubscribableEventSource>map(chain -> new TransformingSubscribableEventSource(
                                 source, chain,
                                 config.getComponent(MessageConverter.class),
                                 config.getComponent(MessageTypeResolver.class)
                         ))
                         .orElse(source);
        }
    }
}
