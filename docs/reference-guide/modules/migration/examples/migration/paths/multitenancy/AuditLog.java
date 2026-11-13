/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.multitenancy;

/**
 * A log that records which tenant an event belonged to, standing in for any component that needs the tenant itself
 * rather than a tenant-scoped resource.
 */
public interface AuditLog {

    void record(String tenantId, String courseId);
}
