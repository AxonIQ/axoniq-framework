/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Describes how a stored snapshot of one {@link VersionedType} is lifted into another, so an entity can be sourced
 * from a snapshot taken under a name or version it is no longer declared with.
 * <p>
 * A transformation is declared as a path of successive edges, each changing the identity of the snapshot, and
 * optionally its shape. An edge without a mapping function carries the state over untouched, which is what a rename
 * is. A path is registered as a whole with a {@link SnapshotTransformerChain}, which splits it into its edges again:
 * <pre>{@code
 * SnapshotTransformation path = SnapshotTransformation
 *         .pathFrom(VersionedType.of("account", "1.0.0"))
 *         .to("2.0.0")
 *         .transform(AccountV1.class, v1 -> new AccountV2(v1.id(), v1.balance()))
 *         .renameTo(VersionedType.of("ledger", "2.0.0"))
 *         .build();
 * }</pre>
 * Each step's target is the next step's source, so an intermediate identity is written once rather than spelled
 * identically in several places. Paths may join: a snapshot stored at any identity along a declared path joins it
 * there.
 * <p>
 * The format a snapshot is stored in belongs to the edge that reads it. By default, the source of an edge is read with
 * the application's current {@link Converter}; {@link EntryStep#storedAs(Converter)} declares another one for the
 * first edge of a path. Only that first edge ever reads stored data: later edges receive the object produced by their
 * predecessor.
 * <p>
 * An aggregate-based snapshot, such as one taken by Axon Framework 4, enters through
 * {@link #pathFromAggregateBased(VersionedType)}. Such a snapshot is stored under the identifier of its aggregate
 * rather than under a name, and is identified by its payload type and revision, where a snapshot without a revision
 * has version {@code 0.0.0}. An aggregate-based entry can only start a path, and always declares the format it is
 * stored in.
 * <p>
 * Snapshots of an identity that should never be used are declared with {@link #rebuild(VersionedType)}, sourcing the
 * entity in full instead.
 * <p>
 * A snapshot that cannot be transformed costs a full replay of the entity's events, and nothing else.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
public sealed interface SnapshotTransformation permits EdgeSnapshotTransformation, RebuildSnapshotTransformation {

    /**
     * Begins a path of successive edges from the given {@code source}.
     *
     * @param source the stored identity the path starts from, cannot be {@code null}
     * @return the entry step, never {@code null}
     * @throws NullPointerException if {@code source} is {@code null}
     */
    static EntryStep pathFrom(VersionedType source) {
        return new EntryStep(VersionedType.of(requireNonNull(source, "source may not be null").qualifiedName(),
                                              source.version()));
    }

    /**
     * Begins a path from an aggregate-based snapshot, such as one taken by Axon Framework 4.
     * <p>
     * The {@code source}'s {@link VersionedType#qualifiedName() name} is the payload type of the Axon Framework 4
     * snapshot, and its {@link VersionedType#version() version} is the revision of that payload type, or
     * {@code 0.0.0} for a snapshot without a revision. The format the snapshot is stored in must be declared next.
     *
     * @param source the payload type and revision of the aggregate-based snapshot, cannot be {@code null}
     * @return the aggregate-based entry step, never {@code null}
     * @throws NullPointerException if {@code source} is {@code null}
     */
    static AggregateBasedEntryStep pathFromAggregateBased(VersionedType source) {
        requireNonNull(source, "source may not be null");

        return new AggregateBasedEntryStep(VersionedType.of(source.qualifiedName(), source.version()));
    }

    /**
     * Declares that snapshots stored as {@code source} must never be used, and that the entity is to be sourced in
     * full instead.
     * <p>
     * Typical uses are quarantining snapshots written before a state-corrupting bug was fixed, and retiring a stored
     * format whose {@link Converter} is no longer worth maintaining.
     *
     * @param source the stored identity to refuse, cannot be {@code null}
     * @return the transformation, never {@code null}
     * @throws NullPointerException if {@code source} is {@code null}
     */
    static SnapshotTransformation rebuild(VersionedType source) {
        VersionedType exactSource = VersionedType.of(requireNonNull(source, "source may not be null").qualifiedName(),
                                                     source.version());

        return new RebuildSnapshotTransformation(exactSource::equals, exactSource.toString());
    }

    /**
     * As {@link #rebuild(VersionedType)}, for an open-ended range of stored identities, such as every {@code 0.x}
     * version of a name.
     *
     * @param sourcePredicate the predicate deciding which stored identities to refuse, cannot be {@code null}
     * @return the transformation, never {@code null}
     * @throws NullPointerException if {@code sourcePredicate} is {@code null}
     */
    static SnapshotTransformation rebuild(Predicate<VersionedType> sourcePredicate) {
        return rebuild(String.valueOf(sourcePredicate), sourcePredicate);
    }

    /**
     * As {@link #rebuild(Predicate)}, with a description of the refused identities for use in log and error messages.
     *
     * @param description     a description of the refused identities, such as {@code "account 0.x"}, cannot be
     *                        {@code null}
     * @param sourcePredicate the predicate deciding which stored identities to refuse, cannot be {@code null}
     * @return the transformation, never {@code null}
     * @throws NullPointerException if any argument is {@code null}
     */
    static SnapshotTransformation rebuild(String description, Predicate<VersionedType> sourcePredicate) {
        return new RebuildSnapshotTransformation(
                requireNonNull(sourcePredicate, "sourcePredicate may not be null"),
                requireNonNull(description, "description may not be null")
        );
    }

    /**
     * The start of a path, where the format its source is stored in may be declared.
     */
    final class EntryStep {

        private final VersionedType source;

        private EntryStep(VersionedType source) {
            this.source = source;
        }

        /**
         * Declares the {@link Converter} the source of this path is stored with, instead of the application's current
         * converter.
         * <p>
         * The first edge then has to map the stored state, which is what reads it, and may change name and version in
         * one step.
         *
         * @param converter the converter the source of this path is stored with, cannot be {@code null}
         * @return the formatted step, never {@code null}
         * @throws NullPointerException if {@code converter} is {@code null}
         */
        public FormattedStep storedAs(Converter converter) {
            return new FormattedStep(source, false, requireNonNull(converter, "converter may not be null"));
        }

        /**
         * Continues by mapping to a new version of the same name.
         *
         * @param version the version to produce, cannot be {@code null}
         * @return the mapping step, never {@code null}
         * @throws NullPointerException     if {@code version} is {@code null}
         * @throws IllegalArgumentException if {@code version} is the source's own version
         */
        public MapStep to(String version) {
            return new MapStep(null, source, false, null, withVersion(source, version));
        }

        /**
         * Continues by changing identity only, carrying the state over untouched.
         *
         * @param target the identity to continue as, cannot be {@code null}
         * @return the next position, never {@code null}
         * @throws NullPointerException     if {@code target} is {@code null}
         * @throws IllegalArgumentException if {@code target} is the source itself
         */
        public PathStep renameTo(VersionedType target) {
            return new PathStep(EdgeSnapshotTransformation.rename(null, source, renameTarget(source, target)));
        }
    }

    /**
     * The start of a path from an aggregate-based snapshot, where the format it is stored in must be declared.
     */
    final class AggregateBasedEntryStep {

        private final VersionedType source;

        private AggregateBasedEntryStep(VersionedType source) {
            this.source = source;
        }

        /**
         * Declares the {@link Converter} the aggregate-based snapshot is stored with, such as one reading the format
         * of the serializer the snapshot was written with.
         *
         * @param converter the converter the snapshot is stored with, cannot be {@code null}
         * @return the formatted step, never {@code null}
         * @throws NullPointerException if {@code converter} is {@code null}
         */
        public FormattedStep storedAs(Converter converter) {
            return new FormattedStep(source, true, requireNonNull(converter, "converter may not be null"));
        }
    }

    /**
     * The start of a path whose stored format has been declared. Its first edge maps the stored state, and may cross
     * to any identity in one step rather than staging at an intermediate one.
     * <p>
     * That first edge always leads to a new identity, so every identity is stored in exactly one format: the source in
     * the declared one, and everything it is lifted to in the application's current one.
     */
    final class FormattedStep {

        private final VersionedType source;
        private final boolean aggregateBased;
        private final Converter converter;

        private FormattedStep(VersionedType source, boolean aggregateBased, Converter converter) {
            this.source = source;
            this.aggregateBased = aggregateBased;
            this.converter = converter;
        }

        /**
         * Continues by mapping to an arbitrary identity, changing name and version together.
         *
         * @param target the identity to produce, cannot be {@code null}
         * @return the mapping step, never {@code null}
         * @throws NullPointerException     if {@code target} is {@code null}
         * @throws IllegalArgumentException if {@code target} is the source itself
         */
        public MapStep to(VersionedType target) {
            return new MapStep(null, source, aggregateBased, converter, renameTarget(source, target));
        }

        /**
         * Continues by mapping to a new version of the same name.
         *
         * @param version the version to produce, cannot be {@code null}
         * @return the mapping step, never {@code null}
         * @throws NullPointerException     if {@code version} is {@code null}
         * @throws IllegalArgumentException if {@code version} is the source's own version
         */
        public MapStep to(String version) {
            return new MapStep(null, source, aggregateBased, converter, withVersion(source, version));
        }
    }

    /**
     * Supplies the mapping for one edge.
     */
    final class MapStep {

        private final @Nullable EdgeSnapshotTransformation previous;
        private final VersionedType source;
        private final boolean aggregateBased;
        private final @Nullable Converter converter;
        private final VersionedType target;

        private MapStep(@Nullable EdgeSnapshotTransformation previous,
                        VersionedType source,
                        boolean aggregateBased,
                        @Nullable Converter converter,
                        VersionedType target) {
            this.previous = previous;
            this.source = source;
            this.aggregateBased = aggregateBased;
            this.converter = converter;
            this.target = target;
        }

        /**
         * Supplies the function lifting the state into its new shape.
         * <p>
         * The mapper sees the entity state alone; the position, timestamp and metadata of the snapshot are preserved.
         * It may decline by returning {@code null}, in which case the entity is sourced in full.
         *
         * @param <T>       the shape of the state before this edge
         * @param <U>       the shape produced
         * @param inputType the type the state is converted to before mapping, cannot be {@code null}
         * @param mapper    the mapping function, cannot be {@code null}
         * @return the next position, never {@code null}
         * @throws NullPointerException if any argument is {@code null}
         */
        public <T, U> PathStep transform(Class<T> inputType,
                                         BiFunction<T, @Nullable ProcessingContext, @Nullable U> mapper) {
            return new PathStep(EdgeSnapshotTransformation.map(
                    previous, source, aggregateBased, converter, target,
                    new SnapshotMapping<>(requireNonNull(inputType, "inputType may not be null"),
                                          requireNonNull(mapper, "mapper may not be null"))
            ));
        }

        /**
         * Supplies the function lifting the state into its new shape, when it needs no {@link ProcessingContext}.
         *
         * @param <T>       the shape of the state before this edge
         * @param <U>       the shape produced
         * @param inputType the type the state is converted to before mapping, cannot be {@code null}
         * @param mapper    the mapping function, cannot be {@code null}
         * @return the next position, never {@code null}
         * @throws NullPointerException if any argument is {@code null}
         * @see #transform(Class, BiFunction)
         */
        public <T, U> PathStep transform(Class<T> inputType, Function<T, @Nullable U> mapper) {
            requireNonNull(mapper, "mapper may not be null");

            return transform(inputType, (T state, @Nullable ProcessingContext context) -> mapper.apply(state));
        }
    }

    /**
     * A position within a path, after at least one edge.
     */
    final class PathStep {

        private final EdgeSnapshotTransformation last;

        private PathStep(EdgeSnapshotTransformation last) {
            this.last = last;
        }

        /**
         * Continues by mapping to a new version of the same name.
         *
         * @param version the version to produce, cannot be {@code null}
         * @return the mapping step, never {@code null}
         * @throws NullPointerException     if {@code version} is {@code null}
         * @throws IllegalArgumentException if {@code version} is the current version on this path
         */
        public MapStep to(String version) {
            return new MapStep(last, last.target(), false, null, withVersion(last.target(), version));
        }

        /**
         * Continues by changing identity only, carrying the state over untouched.
         *
         * @param target the identity to continue as, cannot be {@code null}
         * @return the next position, never {@code null}
         * @throws NullPointerException     if {@code target} is {@code null}
         * @throws IllegalArgumentException if {@code target} is the current identity on this path
         */
        public PathStep renameTo(VersionedType target) {
            return new PathStep(EdgeSnapshotTransformation.rename(last, last.target(),
                                                                  renameTarget(last.target(), target)));
        }

        /**
         * Completes this path as a single transformation, to be registered with a {@link SnapshotTransformerChain}.
         *
         * @return the transformation, never {@code null}
         */
        public SnapshotTransformation build() {
            return last;
        }
    }

    private static VersionedType withVersion(VersionedType source, String version) {
        VersionedType target = VersionedType.of(source.qualifiedName(),
                                                requireNonNull(version, "version may not be null"));

        if (target.equals(source)) {
            throw new IllegalArgumentException("A step must change the version, but it already is: " + source);
        }

        return target;
    }

    private static VersionedType renameTarget(VersionedType source, VersionedType target) {
        VersionedType exactTarget = VersionedType.of(requireNonNull(target, "target may not be null").qualifiedName(),
                                                     target.version());

        if (exactTarget.equals(source)) {
            throw new IllegalArgumentException("A step must change the identity, but it already is: " + source);
        }

        return exactTarget;
    }
}
