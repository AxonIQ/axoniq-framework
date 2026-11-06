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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.axonframework.common.annotation.Internal;

import java.util.List;

/**
 * Classifies the failures that end a consumer of a {@link KafkaSubscribableEventSource}: whether retrying can resolve
 * them, and which ones need a log message of their own.
 * <p>
 * Internal, because it only serves the failure handling of the {@link GroupConsumer}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class ConsumerFailures {

    // In kafka-clients 4, the authentication, authorization, unsupported-version and invalid-topic exceptions extend
    // InvalidConfigurationException; in the 3.x line, they do not, so they are listed explicitly. An instanceof chain
    // would be flagged as always false for them when compiling against kafka-clients 4.
    private static final List<Class<? extends Throwable>> PERMANENT_FAILURES = List.of(
            ConfigException.class,
            InvalidConfigurationException.class,
            AuthenticationException.class,
            AuthorizationException.class,
            UnsupportedVersionException.class,
            InvalidTopicException.class,
            InvalidGroupIdException.class,
            SecurityDisabledException.class
    );

    private ConsumerFailures() {
        // static helper
    }

    /**
     * Returns whether the given {@code failure}, or one of its causes, is one that retrying cannot resolve: invalid
     * settings or topic names, a Kafka client or broker that does not support a requested feature, or failed
     * authentication or authorization.
     *
     * @param failure the failure that ended the consumer
     * @return {@code true} if retrying cannot resolve the failure
     */
    static boolean isPermanent(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (isUnresolvableBootstrapServers(cause)) {
                return false;
            }
            if (isOfPermanentType(cause)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether the given {@code failure}, or one of its causes, reports that another consumer with the same
     * {@code group.instance.id} took this consumer's place in the group.
     *
     * @param failure the failure that ended the consumer
     * @return {@code true} if another consumer fenced this one off
     */
    static boolean isFenced(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof FencedInstanceIdException) {
                return true;
            }
        }
        return false;
    }

    private static boolean isOfPermanentType(Throwable failure) {
        return PERMANENT_FAILURES.stream().anyMatch(type -> type.isInstance(failure));
    }

    /**
     * Returns whether the given {@code failure} reports that none of the bootstrap servers resolves, which the
     * {@code KafkaConsumer} constructor raises as a {@link ConfigException}. The host names may not be registered in
     * DNS yet, for example while the Kafka cluster starts alongside the application, or DNS may be briefly
     * unavailable, so retrying can resolve it. Kafka has no dedicated exception type for it, so the message is
     * matched; it is the same in the 3.x and 4.x lines of kafka-clients.
     *
     * @param failure the failure to classify, not its causes
     * @return {@code true} if the failure reports bootstrap servers that do not resolve
     */
    private static boolean isUnresolvableBootstrapServers(Throwable failure) {
        return failure instanceof ConfigException
                && failure.getMessage() != null
                && failure.getMessage().startsWith("No resolvable bootstrap urls");
    }
}
