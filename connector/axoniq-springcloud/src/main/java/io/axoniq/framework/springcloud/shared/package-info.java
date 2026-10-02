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
 * The parts of the Spring Cloud connector that the command side and the query side both rely on.
 * <p>
 * {@link SpringCloudMemberRegistry} keeps track of the members of the cluster and what each of them handles, for both
 * connectors to route with. {@link SpringCloudAxoniqAddon} identifies the connector to the license entitlement system,
 * and {@link WireCodec} is the encoding of failures both sides write to each other.
 */
@NullMarked
package io.axoniq.framework.springcloud.shared;

import org.jspecify.annotations.NullMarked;
