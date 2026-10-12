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

package io.axoniq.framework.kafka.streaming;

import org.axonframework.messaging.core.annotation.SequencingPolicy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Processes the events an event handler receives from Kafka per partition: in order within a partition, and
 * concurrently across partitions. See {@link KafkaPartitionSequencingPolicy}.
 * <p>
 * Place it on an event handler method, or on a class to apply it to all its event handler methods.
 * <pre>{@code
 * @KafkaPartitionSequencing
 * @EventHandler
 * void on(ParcelDispatched event) {
 *     // ...
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SequencingPolicy(type = KafkaPartitionSequencingPolicy.class)
public @interface KafkaPartitionSequencing {

}
