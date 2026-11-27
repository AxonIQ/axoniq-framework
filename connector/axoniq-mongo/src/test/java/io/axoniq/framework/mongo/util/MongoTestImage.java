/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.util;

/**
 * Names the MongoDB container image used by every test in this module, so the server version under test is defined in a
 * single place.
 * <p>
 * Defaults to {@code mongo:7.0}. MongoDB 8.0 and later refuse to start on Linux kernels 6.19.0 through 7.0.13, which
 * includes the current GitHub-hosted runners. Set the {@code axoniq.mongo.image} system property, for example
 * {@code -Daxoniq.mongo.image=mongo:9.0}, to run the tests against another server version, and raise the default once
 * CI can run the newer one.
 *
 * @author Steven van Beelen
 */
public final class MongoTestImage {

    /**
     * The MongoDB Docker image tests run against.
     */
    public static final String NAME = System.getProperty("axoniq.mongo.image", "mongo:7.0");

    private MongoTestImage() {
        // Test utility class
    }
}
