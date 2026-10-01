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

/**
 * Wiring of the connectors that distribute commands and queries across nodes discovered through Spring Cloud
 * Discovery, without an Axon Server.
 * <p>
 * {@link io.axoniq.framework.springcloud.SpringCloudConfigurationEnhancer} registers both connectors and the components
 * they need; the connectors themselves live in the sub-packages. The pieces are split by concern: {@code command} and
 * {@code query} hold each bus's connector and the HTTP transport carrying its messages between members,
 * {@code shared} holds what both sides rely on, {@code routing} holds the consistent-hash ring and its members, and
 * {@code discovery} learns what each member handles.
 * <p>
 * {@link io.axoniq.framework.springcloud.command.SpringCloudCommandBusConnector} routes each command with the ring that
 * {@link io.axoniq.framework.springcloud.shared.SpringCloudMemberRegistry} maintains from the discovered service
 * instances, while {@link io.axoniq.framework.springcloud.query.SpringCloudQueryBusConnector} rotates each query over
 * the members advertising its name. Both share that one registry, so each publishes what it handles without erasing
 * what the other published.
 */
@NullMarked
package io.axoniq.framework.springcloud;

import org.jspecify.annotations.NullMarked;
