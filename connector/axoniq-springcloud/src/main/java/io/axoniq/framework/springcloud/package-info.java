/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
 * {@link io.axoniq.framework.springcloud.shared.SpringCloudMemberDiscovery} maintains from the discovered service
 * instances, while {@link io.axoniq.framework.springcloud.query.SpringCloudQueryBusConnector} rotates each query over
 * the members advertising its name. Both share that one discovery, so each publishes what it handles without erasing
 * what the other published.
 */
@NullMarked
package io.axoniq.framework.springcloud;

import org.jspecify.annotations.NullMarked;
