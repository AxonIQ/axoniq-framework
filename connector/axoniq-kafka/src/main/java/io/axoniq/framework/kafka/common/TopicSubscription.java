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

package io.axoniq.framework.kafka.common;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The topics a Kafka event source consumes: either a fixed set of topics, or every topic whose name matches a
 * pattern.
 * <p>
 * With a pattern, topics created later are picked up as well, once the source next refreshes its view of the
 * cluster.
 * <pre>{@code
 * TopicSubscription parcels = TopicSubscription.topics("parcels");
 * TopicSubscription allShipping = TopicSubscription.pattern(Pattern.compile("shipping\\..*"));
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public sealed interface TopicSubscription {

    /**
     * Returns a subscription to the given {@code topics}.
     *
     * @param topics the names of the topics to consume, at least one
     * @return a subscription to the given {@code topics}
     * @throws IllegalArgumentException if no topics are given
     */
    static TopicSubscription topics(String... topics) {
        return new Topics(Set.copyOf(Arrays.asList(topics)));
    }

    /**
     * Returns a subscription to every topic whose name matches the given {@code pattern}.
     * <p>
     * A {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource} leaves out the topics whose names
     * end in its dead-letter topic suffix, so that it never consumes its own dead-letter records. A
     * {@link io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource} reads every matching topic, dead-letter
     * topics included; write the pattern so that it excludes them, for example {@code orders\.(?!.*\.DLT$).*}.
     *
     * @param pattern the pattern that the names of the topics to consume match
     * @return a subscription to every topic whose name matches the given {@code pattern}
     */
    static TopicSubscription pattern(Pattern pattern) {
        return new TopicPattern(pattern);
    }

    /**
     * Tells whether this subscription includes the topic with the given {@code topic} name.
     *
     * @param topic the name of a topic
     * @return {@code true} if this subscription includes the topic, {@code false} otherwise
     */
    boolean includes(String topic);

    /**
     * A subscription to a fixed set of topics.
     *
     * @param names the names of the topics
     */
    record Topics(Set<String> names) implements TopicSubscription {

        /**
         * Compact constructor validating that at least one topic is given.
         *
         * @param names the names of the topics
         */
        public Topics {
            names = Set.copyOf(Objects.requireNonNull(names, "The topic names must not be null."));
            if (names.isEmpty()) {
                throw new IllegalArgumentException("At least one topic is required.");
            }
        }

        @Override
        public boolean includes(String topic) {
            return names.contains(topic);
        }
    }

    /**
     * A subscription to every topic whose name matches a pattern.
     *
     * @param pattern the pattern the topic names match
     */
    record TopicPattern(Pattern pattern) implements TopicSubscription {

        /**
         * Compact constructor validating that the pattern is not {@code null}.
         *
         * @param pattern the pattern the topic names match
         */
        public TopicPattern {
            Objects.requireNonNull(pattern, "The pattern must not be null.");
        }

        @Override
        public boolean includes(String topic) {
            return pattern.matcher(topic).matches();
        }
    }
}
