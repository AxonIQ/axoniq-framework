/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * An Axon Framework 4 {@code XStreamSerializer}-compatible {@link org.axonframework.conversion.Converter}, kept only to
 * drain saga state, deadlines and scheduled events that an Axon Framework 4 node serialized with XStream.
 */
@NullMarked
package org.axonframework.conversion.xstream;

import org.jspecify.annotations.NullMarked;
