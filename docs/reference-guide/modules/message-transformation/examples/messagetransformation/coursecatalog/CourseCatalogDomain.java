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

package messagetransformation.coursecatalog;

/**
 * Domain value objects shared by the course catalog transformation samples: the current
 * {@code CoursePublished} event together with its identifier and capacity value objects. These are the
 * live types a typed transformation maps onto, kept separate from the throwaway stored-shape records.
 */
final class CourseCatalogDomain {

    private CourseCatalogDomain() {
    }
}

record CoursePublished(CatalogId catalogId, CourseId courseId, String name, CapacityRange capacity) {
}

record CatalogId(String value) {
}

record CourseId(String value) {
}

record CapacityRange(int min, int max) {
}
