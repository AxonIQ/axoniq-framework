/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License").
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

import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import picocli.CommandLine;

import java.util.Objects;
import java.util.concurrent.CompletionException;

import static io.axoniq.framework.examples.infrastructure.MultiTenancyConfiguration.tenantId;

/**
 * Sends a query to the query bus. See {@link Queries} for the list of supported queries.
 */
@CommandLine.Command(name = "query", aliases = {"q"}, description = "Query course stats.")
public final class SendQuery extends SubCommand {

    @CommandLine.Option(names = {"-q", "--query"},
                        required = true,
                        description = "The query type. Allowed values: ${COMPLETION-CANDIDATES}")
    private Queries query;

    @CommandLine.Option(names = {"-t", "--tenant"},
                        required = true,
                        description = "Tenant to query. Allowed values: ${COMPLETION-CANDIDATES}")
    private Tenants tenant;

    @CommandLine.Option(names = {"-p", "--payload"}, required = false, description = "Payload for query.")
    private String payloadCsv;

    @Override
    public void run() {
        Objects.requireNonNull(query, "Query must be provided.");
        Objects.requireNonNull(tenant, "Tenant must be provided.");

        QueryGateway queryGateway = getComponent(QueryGateway.class);

        var msg = query.apply(payloadCsv)
            .andMetadata(tenantId(tenant.tenantId()));

        try {
            Object result = queryGateway.query(msg, query.responseType()).join();
            echo("[%s] Query %s: %s", tenant.tenantId(), msg, result);
        } catch (CompletionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException(
                    "Failed to query course stats for tenant [%s] and query [%s]: %s".formatted(
                            tenant.tenantId(),
                            msg,
                            cause.getMessage()
                    ),
                    cause
            );
        }
    }
}
