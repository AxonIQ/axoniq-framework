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

package io.axoniq.framework.springcloud.command;

import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.commandhandling.CommandDispatchException;

/**
 * Reports that a command could not be delivered to the {@link Member} it was routed to, or that the member's reply
 * could not be read back.
 * <p>
 * This is the failure that says something about the member's availability, as opposed to a failure the member itself
 * reported: a member that answered is reachable, whatever the answer said. The connector removes a member from its
 * consistent-hash ring on this exception and on no other, so that a command failing for a reason of its own -- a
 * payload no member can read, for instance -- does not empty the ring one member at a time.
 * <p>
 * Raised for a member with no known endpoint, a connection that could not be established, a reply that could not be
 * read, and a member that did not answer within the dispatcher's reply deadline.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class MemberUnreachableException extends CommandDispatchException {

    /**
     * Constructs a {@code MemberUnreachableException} with the given {@code message}.
     *
     * @param message a description of why the member could not be reached
     */
    public MemberUnreachableException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code MemberUnreachableException} with the given {@code message} and {@code cause}.
     *
     * @param message a description of why the member could not be reached
     * @param cause   the failure that prevented the member from being reached
     */
    public MemberUnreachableException(String message, Throwable cause) {
        super(message, cause);
    }
}
