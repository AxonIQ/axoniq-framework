/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.extension.spring.messaging;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.springframework.messaging.Message;

/**
 * Interface describing a mechanism that converts Spring Messages from an Axon Event Messages and vice versa.
 *
 * @author Reda.Housni-Alaoui
 * @since 3.1
 */
public interface EventMessageConverter {

	/**
	 * Converts Axon {@code event} into Spring message.
	 *
	 * @param event The Axon event to convert
	 * @param <T>   The event payload type
	 * @return The outbound Spring message
	 */
	<T> Message convertToOutboundMessage(EventMessage event);

	/**
	 * Converts a Spring inbound {@code message} into an Axon event Message
	 *
	 * @param message The Spring message to convert
	 * @param <T>     The message payload type
	 * @return The inbound Axon event message
	 */
	<T> EventMessage convertFromInboundMessage(Message message);
}
