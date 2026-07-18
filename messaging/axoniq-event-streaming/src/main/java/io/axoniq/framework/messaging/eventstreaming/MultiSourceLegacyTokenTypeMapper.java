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

package io.axoniq.framework.messaging.eventstreaming;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypeMapper;

import java.util.Map;

/**
 * Maps the Axon Framework 4 {@code org.axonframework.eventhandling.MultiSourceTrackingToken} class name to the current
 * {@link MultiSourceTrackingToken}, which moved to the Axoniq Framework. This lets a token store written by Axon
 * Framework 4 be read after upgrading.
 * <p>
 * Marked {@link Internal} for the same reason as the {@link LegacyTokenTypeMapper} it implements: it is a migration
 * bridge, not an application extension point.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 * @deprecated Temporary bridge for reading Axon Framework 4 multi-source token stores after upgrading. Scheduled for
 * removal in 5.5.0, once existing stores have migrated to the Axon Framework 5 token class names.
 */
@Internal
@Deprecated(since = "5.2.0", forRemoval = true)
public final class MultiSourceLegacyTokenTypeMapper implements LegacyTokenTypeMapper {

    /**
     * {@inheritDoc}
     * <p>
     * Maps the Axon Framework 4 {@code MultiSourceTrackingToken} name to the current {@link MultiSourceTrackingToken}.
     */
    @Override
    public Map<String, Class<? extends TrackingToken>> mappings() {
        return Map.of("org.axonframework.eventhandling.MultiSourceTrackingToken", MultiSourceTrackingToken.class);
    }
}
