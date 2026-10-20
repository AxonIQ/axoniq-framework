/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantcommandhandling;

/**
 * A per-tenant notification service, registered as a tenant-scoped component so each tenant notifies
 * through its own configuration.
 */
public interface NotificationService {

    void notifyStudentCourseCreation(String studentId, String courseId);
}
