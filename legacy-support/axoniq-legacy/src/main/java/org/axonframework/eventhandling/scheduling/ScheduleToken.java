/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * Token that identifies a single scheduled Event. This token can be used to refer to a specific scheduled event to
 * cancel that timer.
 * <p>
 * Applications keep tokens in Saga state, so a token written by Axon Framework 4 has to be read on Axon Framework 5
 * nodes. Tokens therefore keep the class names, Java serialization form and Jackson type property of Axon Framework 4.
 *
 * @author Allard Buijze
 * @since 0.7
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface ScheduleToken extends Serializable {

}
