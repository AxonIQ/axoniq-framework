/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventprocessing;

/**
 * A per-tenant projection of enrollments, registered as a tenant-scoped component.
 */
public interface EnrollmentRepository {

    void save(Enrollment enrollment);
}
