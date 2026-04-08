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
import org.axonframework.modelling.entity.domain.development.commands.ChangeMarketeerHubspotUsername;
import org.axonframework.modelling.entity.domain.development.events.MarketeerHubspotUsernameChanged;

public class Marketeer {

    private final String email;
    private String hubspotUsername;

    public Marketeer(String email, String hubspotUsername) {
        this.email = email;
        this.hubspotUsername = hubspotUsername;
    }

    @CommandHandler
    public void handle(ChangeMarketeerHubspotUsername command, EventAppender appender) {
        appender.append(new MarketeerHubspotUsernameChanged(
                command.projectId(),
                command.email(),
                command.hubspotUsername()
        ));
    }

    @EventHandler
    public void on(MarketeerHubspotUsernameChanged event) {
        this.hubspotUsername = event.hubspotUsername();
    }

    public String getEmail() {
        return email;
    }

    public String getHubspotUsername() {
        return hubspotUsername;
    }
}
