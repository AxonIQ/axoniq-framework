/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantqueryhandling;

/**
 * A per-tenant read model of course statistics, registered as a tenant-scoped component.
 */
public interface CourseStatisticsRepository {

    CourseStatistics findById(String courseId);
}
