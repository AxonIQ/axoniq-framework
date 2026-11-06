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

import io.axoniq.framework.kafka.subscribing.KafkaSourceStatus;
import io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Reports the status of the subscribable sources that a {@link KafkaConnector} configures, by name, so that the
 * application can notice a source whose consumers stopped because of a failure that retrying cannot resolve, such as
 * invalid settings or failed authentication or authorization.
 * <p>
 * A subscribable source handles such failures itself: it logs them and stops all its consumers, and the subscribing
 * processor that reads it does not report an error. This component is where that failure surfaces. It reports the
 * status of the {@link KafkaSubscribableEventSource} itself, also when the source is decorated, for example to apply
 * an event transformation. A source that no processor has requested yet reports {@link KafkaSourceStatus.Stopped}.
 * <p>
 * Streamable sources are not reported: their failures fail the event stream of the pooled streaming processor that
 * reads them, and the processor reports and retries them according to its error handling.
 * <p>
 * The connector registers this component. Obtain it from the configuration, or, in
 * Spring, inject it; the Spring Boot autoconfiguration reports it through an Actuator health indicator.
 * <pre>{@code
 * KafkaConnectorStatus status = configuration.getComponent(KafkaConnectorStatus.class);
 * if (!status.healthy()) {
 *     status.sources().forEach((name, source) -> {
 *         if (source instanceof KafkaSourceStatus.Failed failed) {
 *             logger.error("Kafka source [{}] stopped", name, failed.cause());
 *         }
 *     });
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaConnectorStatus {

    private final Map<String, Supplier<KafkaSourceStatus>> sources;

    /**
     * Constructs a status reporting the status that the given {@code sources} supply, by source name. Package-private,
     * as the {@link KafkaConnector} creates it.
     *
     * @param sources the status of each subscribable source, by source name
     */
    KafkaConnectorStatus(Map<String, Supplier<KafkaSourceStatus>> sources) {
        this.sources = Collections.unmodifiableMap(new LinkedHashMap<>(sources));
    }

    /**
     * Returns the current status of every subscribable source, by source name, in the order the sources were
     * configured.
     *
     * @return the current status of every subscribable source, as an unmodifiable map
     */
    public Map<String, KafkaSourceStatus> sources() {
        Map<String, KafkaSourceStatus> result = new LinkedHashMap<>();
        sources.forEach((name, status) -> result.put(name, status.get()));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns the current status of the subscribable source with the given {@code name}.
     *
     * @param name the name of the source
     * @return the status of the source, or an empty {@link Optional} when no subscribable source has that name
     */
    public Optional<KafkaSourceStatus> source(String name) {
        Supplier<KafkaSourceStatus> status = sources.get(Objects.requireNonNull(name, "The name must not be null."));
        return status == null ? Optional.empty() : Optional.of(status.get());
    }

    /**
     * Returns whether no subscribable source has {@link KafkaSourceStatus.Failed failed}.
     *
     * @return {@code true} if no subscribable source has failed
     */
    public boolean healthy() {
        return sources.values().stream().noneMatch(status -> status.get().isFailed());
    }
}
