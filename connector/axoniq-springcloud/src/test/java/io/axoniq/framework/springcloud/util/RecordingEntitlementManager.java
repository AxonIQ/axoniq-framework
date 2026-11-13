/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.util;

import io.axoniq.license.entitlement.AxoniqAddon;
import io.axoniq.license.entitlement.EntitlementConfiguration;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import io.axoniq.license.entitlement.observability.EntitlementObserver;

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
    private final List<Class<? extends AxoniqAddon>> registeredAddons = new CopyOnWriteArrayList<>();

    public List<Claim> claims() {
        return List.copyOf(claims);
    }

    public List<Class<? extends AxoniqAddon>> registeredAddons() {
        return List.copyOf(registeredAddons);
    }

    @Override
    public void setConfiguration(EntitlementConfiguration configuration) {
        // Not relevant to what these tests assert.
    }

    @Override
    public void claimMessage(String addonIdentifier, EntitlementMessageType messageType, int count) {
        claims.add(new Claim(addonIdentifier, messageType, count));
    }

    @Override
    public void registerAddon(Class<? extends AxoniqAddon> addon) {
        registeredAddons.add(addon);
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
