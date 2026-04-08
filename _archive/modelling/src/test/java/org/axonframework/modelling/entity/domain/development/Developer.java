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

package org.axonframework.modelling.entity.domain.development;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.entity.domain.development.commands.ChangeDeveloperGithubUsername;
import org.axonframework.modelling.entity.domain.development.events.DeveloperGithubUsernameChanged;

public record Developer(
        String email,
        String githubUsername
) {

    @CommandHandler
    public void handle(ChangeDeveloperGithubUsername command, EventAppender appender) {
        if (githubUsername.equals(command.githubUsername())) {
            return;
        }
        appender.append(new DeveloperGithubUsernameChanged(
                command.projectId(),
                command.email(),
                githubUsername,
                command.githubUsername()
        ));
    }

    @EventHandler
    public Developer handle(DeveloperGithubUsernameChanged event) {
        return new Developer(event.email(), event.githubUsername());
    }
}
