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
