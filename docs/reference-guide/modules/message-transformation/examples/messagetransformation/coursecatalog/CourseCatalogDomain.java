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
