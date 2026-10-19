/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.routing.MemberCapabilities;

import java.util.Objects;

/**
 * What a member of the cluster reports about itself when asked: who it is, and what it handles.
 * <p>
 * The {@code nodeId} is what identifies a member, rather than anything discovery reports about it. One running
 * application may be reported more than once, for example under two service ids or at two addresses, and is still one
 * member as long as every report leads to the same {@code nodeId}. The same goes for this application's own instance:
 * it is recognized among the discovered instances by answering with this application's own {@code nodeId}.
 *
 * @param nodeId       the identifier of the answering application, unique to its process
 * @param capabilities the messages the answering application handles, and the command load it asks for
 * @author Allard Buijze
 * @since 5.4.0
 */
public record MemberAdvertisement(String nodeId, MemberCapabilities capabilities) {

    /**
     * Compact constructor validating that both the {@code nodeId} and the {@code capabilities} are present.
     *
     * @param nodeId       the identifier of the answering application, unique to its process
     * @param capabilities the messages the answering application handles, and the command load it asks for
     */
    public MemberAdvertisement {
        Objects.requireNonNull(nodeId, "The nodeId must not be null.");
        Objects.requireNonNull(capabilities, "The capabilities must not be null.");
    }
}
