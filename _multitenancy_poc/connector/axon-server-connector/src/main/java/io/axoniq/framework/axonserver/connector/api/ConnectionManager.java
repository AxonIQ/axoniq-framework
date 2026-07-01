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
