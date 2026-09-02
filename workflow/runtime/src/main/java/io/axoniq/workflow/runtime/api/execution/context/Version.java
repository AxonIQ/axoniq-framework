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
package io.axoniq.workflow.runtime.api.execution.context;

import org.axonframework.messaging.core.MessageType;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Workflow version value type. Wraps a semver string ({@code MAJOR.MINOR.PATCH} with optional {@code -prerelease}
 * suffix) together with its parsed semver-aware comparison. Use this in place of raw {@code String} whenever ordering
 * or "is-greater-than" logic is needed; the wire format (e.g. {@link MessageType#version()}) stays {@code String} via
 * {@link #value()}.
 *
 * @author Stefan Dragisic
 * @since 0.2.0
 */
public final class Version implements Comparable<Version> {

    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "^(\\d+)\\.(\\d+)\\.(\\d+)(?:-([0-9A-Za-z.-]+))?$"
    );

    /**
     * The default version assigned to workflow definitions that do not declare one explicitly, aligned with
     * {@link MessageType#DEFAULT_VERSION}.
     */
    public static final Version DEFAULT = Version.of(MessageType.DEFAULT_VERSION);

    private final String value;
    private final int major;
    private final int minor;
    private final int patch;
    private final String preRelease;

    private Version(String value, int major, int minor, int patch, String preRelease) {
        this.value = value;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease;
    }

    /**
     * Parses {@code value} as a semver string and returns the corresponding {@link Version}.
     *
     * @throws IllegalArgumentException if {@code value} is not a valid semver string.
     */
    public static Version of(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Workflow version must not be blank");
        }
        Matcher m = VERSION_PATTERN.matcher(value);
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "Invalid workflow version '" + value + "'. Expected MAJOR.MINOR.PATCH"
                            + " with optional -prerelease suffix (e.g. 0.0.1, 1.2.3-rc1)."
            );
        }
        return new Version(value,
                           Integer.parseInt(m.group(1)),
                           Integer.parseInt(m.group(2)),
                           Integer.parseInt(m.group(3)),
                           m.group(4));
    }

    /**
     * Validates {@code value} is a parseable semver string. Throws otherwise.
     */
    public static void validate(String value) {
        of(value);
    }

    /**
     * Parses {@code value} as a semver string or returns empty when unparseable. Useful for filtering collections of
     * mixed-quality version strings (e.g. legacy event streams).
     */
    public static Optional<Version> tryOf(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(of(value));
        } catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }

    /**
     * Returns the <strong>highest version less than or equal to</strong> {@code target} among {@code candidates}, or
     * empty when no candidate qualifies. This is the "closest-sibling" routing rule.
     */
    public static Optional<Version> closestNotGreaterThan(Collection<Version> candidates,
                                                          Version target) {
        return candidates.stream()
                         .filter(v -> v.compareTo(target) <= 0)
                         .max(Version::compareTo);
    }

    /**
     * Returns the <strong>lowest version strictly greater than</strong> {@code target} among {@code candidates}, or
     * empty when no candidate qualifies. This is the "closest-higher-sibling" routing rule.
     */
    public static Optional<Version> closestHigherThan(Collection<Version> candidates,
                                                      Version target) {
        return candidates.stream()
                         .filter(v -> v.compareTo(target) > 0)
                         .min(Version::compareTo);
    }

    /**
     * The canonical {@code String} representation, suitable for {@link MessageType#version()} and other wire-format
     * boundaries.
     */
    public String value() {
        return value;
    }

    /**
     * {@code true} when this version is strictly greater than {@code other} in semver order.
     */
    public boolean isGreaterThan(Version other) {
        return compareTo(other) > 0;
    }

    /**
     * {@code true} when this version is greater than or equal to {@code other} in semver order.
     */
    public boolean isGreaterThanOrEqualTo(Version other) {
        return compareTo(other) >= 0;
    }

    @Override
    public int compareTo(Version other) {
        int cmp = Integer.compare(this.major, other.major);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.minor, other.minor);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.patch, other.patch);
        if (cmp != 0) {
            return cmp;
        }
        return comparePreRelease(this.preRelease, other.preRelease);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Version that)) {
            return false;
        }
        return this.compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch, preRelease);
    }

    @Override
    public String toString() {
        return value;
    }

    private static int comparePreRelease(String left, String right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        String[] l = left.split("\\.");
        String[] r = right.split("\\.");
        int len = Math.min(l.length, r.length);
        for (int i = 0; i < len; i++) {
            int cmp = comparePreReleasePart(l[i], r[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(l.length, r.length);
    }

    private static int comparePreReleasePart(String left, String right) {
        boolean leftNumeric = isNumeric(left);
        boolean rightNumeric = isNumeric(right);
        if (leftNumeric && rightNumeric) {
            return Integer.compare(Integer.parseInt(left), Integer.parseInt(right));
        }
        if (leftNumeric) {
            return -1;
        }
        if (rightNumeric) {
            return 1;
        }
        return left.compareTo(right);
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
