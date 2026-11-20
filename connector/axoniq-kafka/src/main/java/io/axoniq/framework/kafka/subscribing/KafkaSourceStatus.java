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
package io.axoniq.framework.kafka.subscribing;

import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Objects;

/**
 * The status of a {@link KafkaSubscribableEventSource}: whether its consumers run, have stopped, or have stopped
 * because of a failure that retrying cannot resolve.
 * <p>
 * Transient failures, such as an unreachable broker, are retried for as long as the consumers run, and leave the
 * status {@link Running}. A running source may still have stopped consuming single partitions, for example at a record
 * it cannot decode; {@link Running#stoppedPartitions()} lists those. Obtain the status of every subscribable source of
 * the connector through the {@link io.axoniq.framework.kafka.configuration.KafkaConnectorStatus} component, for
 * example in a health check:
 * <pre>{@code
 * switch (configuration.getComponent(KafkaConnectorStatus.class).source("parcelGroup").orElseThrow()) {
 *     case KafkaSourceStatus.Failed failed -> alert(failed.cause());
 *     case KafkaSourceStatus.Running running when !running.stoppedPartitions().isEmpty() ->
 *             warn(running.stoppedPartitions());
 *     default -> { }
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public interface KafkaSourceStatus {

    /**
     * Returns whether this status reports a failure.
     *
     * @return {@code true} if this status is {@link Failed}
     */
    default boolean isFailed() {
        return this instanceof Failed;
    }

    /**
     * The source's consumers run: a processor is subscribed, and the source has started. The consumers may have
     * stopped consuming single partitions, which {@link #stoppedPartitions()} lists.
     */
    final class Running implements KafkaSourceStatus {

        private final List<StoppedPartition> stoppedPartitions;

        /**
         * Constructs the status of a source whose consumers run, but do not progress on the given
         * {@code stoppedPartitions}.
         *
         * @param stoppedPartitions the partitions the consumers do not progress on
         */
        Running(List<StoppedPartition> stoppedPartitions) {
            this.stoppedPartitions = List.copyOf(
                    Objects.requireNonNull(stoppedPartitions, "The stoppedPartitions must not be null.")
            );
        }

        /**
         * Returns the partitions that the source's consumers in this application instance own, but do not progress
         * on, with the reason. Consumption of a partition stops at a record that the
         * {@link io.axoniq.framework.kafka.codec.UndecodableRecordPolicy#FAIL} policy rejects, and at a committed
         * offset lost to retention under {@link io.axoniq.framework.kafka.common.RetentionGapPolicy#FAIL}; it does not
         * progress while the {@link FailedRecordPolicy#RETRY} policy retries a record. Other partitions are not
         * affected. A partition leaves the list once it progresses again, or moves to another consumer.
         *
         * @return the partitions the source does not progress on, as an unmodifiable list
         */
        public List<StoppedPartition> stoppedPartitions() {
            return stoppedPartitions;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Running that && stoppedPartitions.equals(that.stoppedPartitions);
        }

        @Override
        public int hashCode() {
            return stoppedPartitions.hashCode();
        }

        @Override
        public String toString() {
            return "Running{stoppedPartitions=" + stoppedPartitions + "}";
        }
    }

    /**
     * The source's consumers do not run: the source has not started or has shut down, or no processor is subscribed.
     */
    final class Stopped implements KafkaSourceStatus {

        /**
         * Constructs the status of a source whose consumers do not run.
         * <p>
         * Internal, because sources report their own status; the
         * {@link io.axoniq.framework.kafka.configuration.KafkaConnector} uses it for a source that is not built yet.
         */
        @Internal
        public Stopped() {
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Stopped;
        }

        @Override
        public int hashCode() {
            return Stopped.class.hashCode();
        }

        @Override
        public String toString() {
            return "Stopped";
        }
    }

    /**
     * A consumer of the source failed in a way that retrying cannot resolve, such as invalid settings, failed
     * authentication or authorization, or an {@link Error} on its polling thread, and the source stopped all its
     * consumers. The status stays failed until the source starts consuming again, for example after the application
     * restarts.
     */
    final class Failed implements KafkaSourceStatus {

        private final Throwable cause;

        /**
         * Constructs the status of a source that stopped because of the given {@code cause}.
         *
         * @param cause the failure that stopped the consumers
         */
        Failed(Throwable cause) {
            this.cause = Objects.requireNonNull(cause, "The cause must not be null.");
        }

        /**
         * Returns the failure that stopped the consumers.
         *
         * @return the failure that stopped the consumers
         */
        public Throwable cause() {
            return cause;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Failed that && cause.equals(that.cause);
        }

        @Override
        public int hashCode() {
            return cause.hashCode();
        }

        @Override
        public String toString() {
            return "Failed{cause=" + cause + "}";
        }
    }
}
