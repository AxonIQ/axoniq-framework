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

package org.axonframework.common;

import java.util.UUID;

/**
 * Default IdentifierFactory implementation that uses generates random {@code java.util.UUID} based identifiers.
 * Although the performance of this strategy is not the best out there, it has native supported on all JVMs.
 * <p/>
 * This implementations selects a random identifier out of about 3 x 10<sup>38</sup> possible values, making the chance
 * to get a duplicate incredibly small.
 *
 * @author Allard Buijze
 * @since 1.2
 */
public class DefaultIdentifierFactory extends IdentifierFactory {

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation creates identifiers based on pseudo-random UUIDs.
     */
    @Override
    public String generateIdentifier() {
        return UUID.randomUUID().toString();
    }
}
