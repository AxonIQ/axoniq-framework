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

package io.axoniq.framework.examples.cli;

import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Message;
import picocli.CommandLine;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.examples.infrastructure.MultiTenancyConfiguration.tenantId;

/**
 * Command to send a command message to the command bus. See {@link Commands} for the list of supported commands.
 */
@CommandLine.Command(name = "command", aliases = {"c"}, description = "Publish a command message to the command bus.")
public final class SendCommand extends SubCommand {

    @CommandLine.Option(names = {"-c", "--command"},
                        required = true,
                        description = "The command type. Allowed values: ${COMPLETION-CANDIDATES}")
    private Commands command;

    @CommandLine.Option(names = {"-t", "--tenant"},
                        required = true,
                        description = "Tenant to publish to. Allowed values: ${COMPLETION-CANDIDATES}")
    private Tenants tenant;

    @CommandLine.Option(names = {"-p",
            "--payload"}, required = true, description = "Command payload to publish. CSV, parsed by cli.")
    private String payloadCsv;

    @Override
    public void run() {
        Objects.requireNonNull(command, "Command must be provided.");
        Objects.requireNonNull(tenant, "Tenant must be provided.");
        Objects.requireNonNull(payloadCsv, "Payload must be provided.");
        var commandGateway = getComponent(CommandGateway.class);

        var cmd = command.apply(payloadCsv);
        var result = commandGateway.send(
                cmd,
                tenantId(tenant.tenantId())
        );

        try {
            Message response = result.getResultMessage().orTimeout(30, TimeUnit.SECONDS).join();
            echo("[%s] Published command %s%s",
                 tenant.tenantId(),
                 cmd,
                 (response != null && response.payload() != null) ? " -> " + response.payload() : "");
        } catch (CompletionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException(
                    "Failed to publish command for tenant [%s] and command [%s]: %s".formatted(
                            tenant.tenantId(),
                            cmd,
                            cause.getMessage()
                    ),
                    cause
            );
        }
    }
}
