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

package org.axonframework.update.api;

import org.axonframework.common.annotation.Internal;

/**
 * Represents an artifact with its group ID, artifact ID, and version.
 *
 * @param groupId    The group ID of the artifact.
 * @param artifactId The artifact ID of the artifact.
 * @param version    The version of the artifact.
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public record Artifact(
        String groupId,
        String artifactId,
        String version
) {

    /**
     * Returns a short version of the group ID, to save bytes over the wire.
     *
     * @return The short version of the group ID, or the original if it can't be shortened.
     */
    public String shortGroupId() {
        if (groupId.startsWith("org.axonframework.extensions")) {
            if(groupId.length() == 28) {
                return "ext";
            }
            return "ext." + groupId.substring(29);
        }
        if (groupId.startsWith("org.axonframework")) {
            if(groupId.length() == 17) {
                return "fw";
            }
            return "fw." + groupId.substring(18);
        }
        if (groupId.startsWith("io.axoniq")) {
            if(groupId.length() == 9) {
                return "iq";
            }
            return "iq." + groupId.substring(10);
        }
        return groupId;
    }

}
