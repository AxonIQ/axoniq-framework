/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.api;

import java.util.Map;

/**
 * Interface describing functionality for connection managers. A connection manager typically deals with a collection of
 * connections per context the application is wired with.
 *
 * @author Steven van Beelen
 * @author Milan Savic
 * @author Sara Pelligrini
 * @since 4.6.0
 */
public interface ConnectionManager {

    /**
     * Return the connections this instances manages. Consists of key-value pairs where the key resembles the context
     * name and the value describes whether the connection is active at this moment.
     *
     * @return Return the connections this instances manages.
     */
    Map<String, Boolean> connections();
}
