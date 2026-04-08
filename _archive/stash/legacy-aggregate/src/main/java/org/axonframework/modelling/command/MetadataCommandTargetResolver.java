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

package org.axonframework.modelling.command;

import org.axonframework.messaging.commandhandling.CommandMessage;

/**
 * CommandTargetResolver implementation that uses Metadata entries to extract the identifier.
 * <p/>
 * While this may require duplication of data (as the identifier is already included in the payload as well), it is a
 * more performing alternative to a reflection based CommandTargetResolvers.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class MetadataCommandTargetResolver implements CommandTargetResolver {

    private final String identifierKey;

    /**
     * Initializes the MetadataCommandTargetResolver to use the given {@code identifierKey} as the Metadata key to the
     * aggregate identifier.
     * <p/>
     * When the given {@code identifierKey} is not present in a command's Metadata,
     * {@link #resolveTarget(CommandMessage)} will raise an {@link IllegalArgumentException}
     *
     * @param identifierKey The key of the metadata field containing the aggregate identifier
     */
    public MetadataCommandTargetResolver(String identifierKey) {
        this.identifierKey = identifierKey;
    }

    @Override
    public String resolveTarget(CommandMessage command) {
        String identifier = command.metadata().get(identifierKey).toString();
        if (identifier == null) {
            throw new IdentifierMissingException(
                    "The Metadata for the command does not contain an identifier under key [" + identifierKey + "]"
            );
        }
        return identifier;
    }
}
