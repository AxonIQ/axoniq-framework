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
package io.axoniq.framework.springboot.actuator.kafka;

import io.axoniq.framework.kafka.configuration.KafkaConnectorStatus;
import io.axoniq.framework.kafka.subscribing.KafkaSourceStatus;
import io.axoniq.framework.kafka.subscribing.StoppedPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AbstractHealthIndicator} reporting the status of the subscribable sources of the Kafka connector, as the
 * {@link KafkaConnectorStatus} component provides it. This status is exposed through the {@code "kafkaConnector"}
 * component.
 * <p>
 * The status is {@link Status#DOWN} when a subscribable source has stopped because of a failure that retrying cannot
 * resolve, such as invalid settings or failed authentication or authorization, and {@link Status#UP} otherwise. The
 * details hold the status of every subscribable source by name, and the cause of each failure. For a running source,
 * they also list the partitions it does not progress on, with the reason, such as a record the source cannot decode or
 * a record it retries; those leave the status {@link Status#UP}.
 * Streamable sources are not reported: the pooled streaming processors that read them report their failures.
 * <p>
 * The {@link io.axoniq.framework.springboot.autoconfig.KafkaActuatorAutoConfiguration} registers this indicator when
 * Spring Boot Actuator is on the classpath. Disable it with {@code management.health.kafka-connector.enabled=false}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaConnectorHealthIndicator extends AbstractHealthIndicator {

    private final ObjectProvider<KafkaConnectorStatus> status;

    /**
     * Constructs a health indicator reporting the status that the given {@code status} provider supplies. The status
     * is looked up on every health check. Without a status, for example when the connector is not used, the indicator
     * reports {@link Status#UP}.
     *
     * @param status the provider of the Kafka connector's status
     */
    public KafkaConnectorHealthIndicator(ObjectProvider<KafkaConnectorStatus> status) {
        super("Checking the Kafka connector's health failed");
        this.status = Objects.requireNonNull(status, "The status must not be null.");
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        KafkaConnectorStatus current = status.getIfAvailable();
        if (current == null) {
            builder.up();
            return;
        }
        Map<String, Object> sources = new LinkedHashMap<>();
        boolean failed = false;
        for (Map.Entry<String, KafkaSourceStatus> source : current.sources().entrySet()) {
            Map<String, Object> details = new LinkedHashMap<>();
            switch (source.getValue()) {
                case KafkaSourceStatus.Running running -> {
                    details.put("status", "RUNNING");
                    if (!running.stoppedPartitions().isEmpty()) {
                        details.put("stoppedPartitions",
                                    running.stoppedPartitions().stream().map(KafkaConnectorHealthIndicator::describe)
                                           .toList());
                    }
                }
                case KafkaSourceStatus.Stopped stopped -> details.put("status", "STOPPED");
                case KafkaSourceStatus.Failed failure -> {
                    failed = true;
                    details.put("status", "FAILED");
                    details.put("error", describe(failure.cause()));
                }
                default -> details.put("status", source.getValue().toString());
            }
            sources.put(source.getKey(), details);
        }
        builder.status(failed ? Status.DOWN : Status.UP).withDetail("sources", sources);
    }

    private static Map<String, Object> describe(StoppedPartition stopped) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("partition", stopped.partition().toString());
        details.put("offset", stopped.offset());
        details.put("reason", stopped.reason().name());
        if (stopped.reason() == StoppedPartition.Reason.RETRYING) {
            details.put("attempts", stopped.attempts());
        }
        Throwable cause = stopped.cause();
        if (cause != null) {
            details.put("error", describe(cause));
        }
        return details;
    }

    private static String describe(Throwable failure) {
        return failure.getClass().getName() + ": " + failure.getMessage();
    }
}
