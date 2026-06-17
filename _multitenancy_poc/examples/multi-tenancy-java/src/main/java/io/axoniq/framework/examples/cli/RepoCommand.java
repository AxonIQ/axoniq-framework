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

import io.axoniq.framework.examples.faculty.read.coursestats.CourseStatsRepository;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import picocli.CommandLine;

import java.util.Objects;

/**
 * Plain access to the in-memory repositories, used for debugging purposes.
 */
@CommandLine.Command(name = "repo", aliases = {"r"}, description = "Direct access to inMem repo")
public class RepoCommand extends SubCommand {

    @CommandLine.Option(names = {"-t", "--tenant"},
                        required = true,
                        description = "Tenant to query. Allowed values: ${COMPLETION-CANDIDATES}")
    private Tenants tenant;

    @Override
    public void run() {
        Objects.requireNonNull(tenant, "Tenant must be provided.");

        TenantComponentRegistry<CourseStatsRepository> registry = getComponent(TenantComponentRegistry.class);
        CourseStatsRepository repository = registry.getComponent(tenant.tenantDescriptor());
        echo("[%s] %s", tenant.tenantId(), repository);
    }
}
