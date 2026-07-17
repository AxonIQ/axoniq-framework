package messagetransformation.coursecatalog;

import tools.jackson.databind.JsonNode;

/**
 * Supporting example mapper referenced by the overlap-resolution sample on the configuring page. Maps the
 * historic {@code 0.9.0} beta welcome message onto version {@code 1.0.0} with its own dedicated rule.
 */
public final class WelcomeMessage090ToV1 {

    private WelcomeMessage090ToV1() {
    }

    /**
     * Maps the {@code 0.9.0} beta payload onto the {@code 1.0.0} shape.
     *
     * @param payload the stored {@code 0.9.0} payload
     * @return the {@code 1.0.0} payload
     */
    public static JsonNode map(JsonNode payload) {
        return payload;
    }
}
