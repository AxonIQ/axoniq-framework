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

package migration.paths.multitenancy;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

public class TenantAuditProjection {

    private final AuditLog auditLog;

    public TenantAuditProjection(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    // tag::read-tenant-from-context[]
    @EventHandler
    public void on(CourseCreated event, ProcessingContext context) {
        TenantDescriptor tenant = TenantDescriptor.fromContext(context).orElseThrow();
        auditLog.record(tenant.tenantId(), event.courseId());
    }
    // end::read-tenant-from-context[]
}
