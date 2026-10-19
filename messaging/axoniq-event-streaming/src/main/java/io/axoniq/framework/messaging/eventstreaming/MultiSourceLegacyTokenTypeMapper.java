/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
