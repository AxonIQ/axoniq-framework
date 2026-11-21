/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.util;

import org.axonframework.common.annotation.Internal;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The rules a workflow id must follow: at least one and at most {@value #MAX_LENGTH} characters, each of them
 * printable ASCII ({@code 0x20} to {@code 0x7E}, the space included).
 * <p>
 * The workflow engine applies these rules whenever a
 * {@link io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider} derives the id of a new
 * workflow instance: it does not start an instance with an invalid id and logs an error instead. The rules keep every
 * id storable in a {@code VARCHAR(255)} column, where each character takes one byte in any database character set, and
 * readable in logs and by operators. An empty id is invalid, as it identifies nothing. To use a longer or non-ASCII
 * business key, derive a valid id from it, for example its SHA-256 hash as 64 hexadecimal characters.
 * <p>
 * Internal, as it serves the workflow engine only; the rules themselves are documented on
 * {@link io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
public final class WorkflowIds {

    /**
     * The maximum number of characters of a workflow id.
     */
    public static final int MAX_LENGTH = 255;

    /**
     * The maximum number of characters of a workflow id that {@link #abbreviate(String)} keeps.
     */
    static final int ABBREVIATED_LENGTH = 64;

    private static final char FIRST_PRINTABLE = 0x20;
    private static final char LAST_PRINTABLE = 0x7E;

    private WorkflowIds() {
    }

    /**
     * Returns why the given workflow id is invalid, or an empty {@link Optional} when it is valid.
     *
     * @param workflowId the workflow id to validate
     * @return the reason the id is invalid, naming the violated rule, or an empty {@code Optional} when it is valid
     */
    public static Optional<String> invalidity(String workflowId) {
        requireNonNull(workflowId, "The workflow id must not be null.");
        if (workflowId.isEmpty()) {
            return Optional.of("it is empty");
        }
        if (workflowId.length() > MAX_LENGTH) {
            return Optional.of("it has " + workflowId.length() + " characters, more than the maximum of " + MAX_LENGTH);
        }
        for (int i = 0; i < workflowId.length(); i++) {
            char c = workflowId.charAt(i);
            if (c < FIRST_PRINTABLE || c > LAST_PRINTABLE) {
                return Optional.of(String.format(
                        "the character at index %d (U+%04X) is not printable ASCII (0x20 to 0x7E)",
                        i, workflowId.codePointAt(i)
                ));
            }
        }
        return Optional.empty();
    }

    /**
     * Returns whether the given workflow id is valid, see {@link #invalidity(String)}.
     *
     * @param workflowId the workflow id to validate
     * @return {@code true} when the id is valid
     */
    public static boolean isValid(String workflowId) {
        return invalidity(workflowId).isEmpty();
    }

    /**
     * Returns the given workflow id bounded for a log message: the id itself when it has at most
     * {@value #ABBREVIATED_LENGTH} characters, otherwise its first {@value #ABBREVIATED_LENGTH} characters followed by
     * {@code "..."}. Code points that are not printable ASCII are escaped as {@code \}{@code u} followed by their
     * hexadecimal value, a surrogate pair as the one code point it encodes, so the result is always printable.
     *
     * @param workflowId the workflow id to abbreviate
     * @return the bounded, printable form of the id
     */
    public static String abbreviate(String workflowId) {
        requireNonNull(workflowId, "The workflow id must not be null.");
        int length = Math.min(workflowId.length(), ABBREVIATED_LENGTH);
        StringBuilder result = new StringBuilder(length + 3);
        int i = 0;
        while (i < length) {
            int codePoint = workflowId.codePointAt(i);
            if (codePoint < FIRST_PRINTABLE || codePoint > LAST_PRINTABLE) {
                result.append(String.format("\\u%04X", codePoint));
            } else {
                result.append((char) codePoint);
            }
            i += Character.charCount(codePoint);
        }
        if (i < workflowId.length()) {
            result.append("...");
        }
        return result.toString();
    }
}
