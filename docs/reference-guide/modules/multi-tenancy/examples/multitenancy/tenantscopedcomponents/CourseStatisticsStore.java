/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantscopedcomponents;

/**
 * A per-tenant read model of course statistics, standing in for whatever tenant-scoped resource an
 * application owns.
 */
public interface CourseStatisticsStore {

    void save(CourseStatistics statistics);

    void flush();

    void close();
}
