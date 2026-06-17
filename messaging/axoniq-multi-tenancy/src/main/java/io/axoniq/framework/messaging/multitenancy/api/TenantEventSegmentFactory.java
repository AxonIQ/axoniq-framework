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
package io.axoniq.framework.messaging.multitenancy.api;


import org.axonframework.eventsourcing.eventstore.EventStore;

import java.util.function.Function;

/**
 * Factory for creating {@link EventStore} segments for a given {@link TenantDescriptor}. After a segment is created, it
 * may be started automatically by the factory.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */
@FunctionalInterface
public interface TenantEventSegmentFactory extends Function<TenantDescriptor, EventStore> {

}
