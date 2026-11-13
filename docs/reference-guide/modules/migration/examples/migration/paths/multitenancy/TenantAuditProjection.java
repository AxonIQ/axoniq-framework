/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
