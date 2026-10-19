/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.annotation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

/**
 * Interface describing a message handler capable of handling a specific deadline.
 *
 * @param <T> the type of entity to which the message handler will delegate the actual handling of the deadline
 * @author Milan Savic
 * @since 3.3.0
 */
@Internal
public interface DeadlineHandlingMember<T> extends MessageHandlingMember<T> {

}
