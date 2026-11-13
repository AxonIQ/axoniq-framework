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

package io.axoniq.framework.kafka.utils;

import io.axoniq.license.entitlement.AxoniqAddon;
import io.axoniq.license.entitlement.EntitlementConfiguration;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import io.axoniq.license.entitlement.observability.EntitlementObserver;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An {@link EntitlementManager} for tests, recording the claims made against it.
 * <p>
 * Stands in for {@link EntitlementManager#INSTANCE} so a test need not touch that singleton, nor depend on a license
 * being present.
 *
 * @author Allard Buijze
 */
public class RecordingEntitlementManager implements EntitlementManager {

    private final List<Claim> claims = new CopyOnWriteArrayList<>();
    private volatile @Nullable RuntimeException claimFailure;

    public List<Claim> claims() {
        return List.copyOf(claims);
    }

    /**
     * Makes every later claim throw the given {@code failure}, after recording it, as an entitlement manager does when
     * the licensed throughput is exceeded.
     *
     * @param failure the exception claims throw
     */
    public void failClaimsWith(RuntimeException failure) {
        this.claimFailure = failure;
    }

    @Override
    public void setConfiguration(EntitlementConfiguration configuration) {
        // Not relevant to what these tests assert.
    }

    @Override
    public void claimMessage(String addonIdentifier, EntitlementMessageType messageType, int count) {
        claims.add(new Claim(addonIdentifier, messageType, count));
        RuntimeException failure = claimFailure;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void registerAddon(Class<? extends AxoniqAddon> addon) {
        // Not relevant to what these tests assert.
    }

    @Override
    public void registerObserver(EntitlementObserver observer) {
        // Not relevant to what these tests assert.
    }

    @Override
    public void removeObserver(EntitlementObserver observer) {
        // Not relevant to what these tests assert.
    }

    /**
     * One claim made against this manager.
     */
    public record Claim(String addonIdentifier, EntitlementMessageType messageType, int count) {

    }
}
