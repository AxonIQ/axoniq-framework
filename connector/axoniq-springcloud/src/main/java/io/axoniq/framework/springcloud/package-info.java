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
 * Connectors distributing commands and queries across nodes discovered through Spring Cloud Discovery, without an
 * Axon Server.
 * <p>
 * {@link io.axoniq.framework.springcloud.SpringCloudCommandBusConnector} routes each command with the consistent-hash
 * ring that {@link io.axoniq.framework.springcloud.SpringCloudMemberRegistry} maintains from the discovered service
 * instances, while {@link io.axoniq.framework.springcloud.SpringCloudQueryBusConnector} rotates each query over the
 * members advertising its name. Both share that one registry, so each publishes what it handles without erasing what
 * the other published. {@link io.axoniq.framework.springcloud.SpringCloudConfigurationEnhancer} wires them in.
 * <p>
 * The pieces live in sub-packages by concern: {@code routing} holds the ring and its members, {@code discovery} learns
 * what each member handles, and {@code transport} carries messages between them over HTTP.
 */
@NullMarked
package io.axoniq.framework.springcloud;

import org.jspecify.annotations.NullMarked;
