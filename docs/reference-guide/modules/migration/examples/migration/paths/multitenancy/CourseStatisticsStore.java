/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.multitenancy;

/**
 * The read model a projection writes through, held per tenant by a {@code TenantComponentProvider}.
 */
public interface CourseStatisticsStore extends AutoCloseable {

    void save(CourseStatistics statistics);
}
