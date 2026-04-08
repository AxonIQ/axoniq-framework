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
 * Represents an upgrade suggestion for a specific artifact version by the AxonIQ UpdateChecker API.
 *
 * @param groupId         The group ID of the library.
 * @param artifactId      The artifact ID of the library.
 * @param latestVersion   The latest version of the library available for upgrade.
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public record ArtifactAvailableUpgrade(
        String groupId,
        String artifactId,
        String latestVersion
) {

}
