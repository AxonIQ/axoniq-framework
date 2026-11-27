/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.bson.Document;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mongo entity for an {@link Instant} such that no precision is lost.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class InstantEntry {

    static final String SECONDS_KEY = "seconds";
    static final String NANOSECONDS_KEY = "nanoseconds";
    static final Set<String> KEY_SET = Stream.of(SECONDS_KEY, NANOSECONDS_KEY)
                                             .collect(Collectors.toCollection(HashSet::new));

    private final Instant instant;

    /**
     * The {@link Instant} value represented by this entry.
     *
     * @return the {@link Instant} value.
     */
    public Instant getInstant() {
        return instant;
    }

    /**
     * Initializes an {@code InstantEntry} entry using a DBObject containing the Mongo Document. Throws
     * {@link NotAnInstantDocumentException} if the provided document does not have only {@code seconds} and
     * {@code nanoseconds} as keys.
     *
     * @param dbInstantEntry the {@link Document} representation of the entity.
     */
    public InstantEntry(Document dbInstantEntry) {
        Set<String> entryKeys = dbInstantEntry.keySet();
        if (!entryKeys.equals(KEY_SET)) {
            throw new NotAnInstantDocumentException(
                    String.format("Actual keys on the document are %s and not %s.", entryKeys, KEY_SET)
            );
        }
        long seconds = dbInstantEntry.getLong(SECONDS_KEY);
        int nanoseconds = dbInstantEntry.getInteger(NANOSECONDS_KEY);
        this.instant = Instant.ofEpochSecond(seconds, nanoseconds);
    }

    /**
     * Initializes a {@code InstantEntry} entry using an {@link Instant}.
     *
     * @param instant the {@link Instant} representation of the entity.
     */
    public InstantEntry(Instant instant) {
        this.instant = instant;
    }

    /**
     * Returns the instant as a mongo Document.
     *
     * @return Document representing the instant.
     */
    public Document asDocument() {
        return new Document().append(SECONDS_KEY, instant.getEpochSecond())
                             .append(NANOSECONDS_KEY, instant.getNano());
    }
}
