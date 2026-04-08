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

package org.axonframework.extension.spring.event;

/**
 * Event published after initialization of Axon through the Spring event mechanism. Indicates that Axon is fully started
 * and can handle all commands, events and queries.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class AxonStartedEvent {

}
