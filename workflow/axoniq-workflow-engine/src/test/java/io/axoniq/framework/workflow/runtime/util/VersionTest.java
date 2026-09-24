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

package io.axoniq.framework.workflow.runtime.util;

import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link Version}, the workflow version value type (semver parsing, ordering and the closest-sibling routing
 * helpers). Supersedes the former {@code WorkflowVersionComparator} tests.
 *
 * @author Stefan Dragisic
 */
class VersionTest {

    @ParameterizedTest
    @CsvSource({
            "0.0.1, 0.0.2",
            "0.0.1, 0.1.0",
            "0.0.1, 1.0.0",
            "1.2.0, 1.10.0",
            "1.9.0, 1.10.0",
            "2.0.0, 10.0.0",
            "1.0.0-alpha, 1.0.0",
            "1.0.0-alpha, 1.0.0-beta",
            "1.0.0-alpha.1, 1.0.0-alpha.2",
            "1.0.0-alpha.2, 1.0.0-alpha.10",
            "1.0.0-rc.1, 1.0.0"
    })
    void orderingIsSemver(String lower, String higher) {
        Version low = Version.of(lower);
        Version high = Version.of(higher);
        assertThat(low.compareTo(high)).isNegative();
        assertThat(high.compareTo(low)).isPositive();
        assertThat(high.isGreaterThan(low)).isTrue();
        assertThat(low.isGreaterThan(high)).isFalse();
        assertThat(high.isGreaterThanOrEqualTo(low)).isTrue();
        assertThat(low.isGreaterThanOrEqualTo(high)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "0.0.1, 0.0.1",
            "1.2.3, 1.2.3",
            "1.0.0-alpha.1, 1.0.0-alpha.1"
    })
    void equalVersionsCompareEqual(String left, String right) {
        Version l = Version.of(left);
        Version r = Version.of(right);
        assertThat(l.compareTo(r)).isZero();
        assertThat(l).isEqualTo(r);
        assertThat(l.hashCode()).isEqualTo(r.hashCode());
        assertThat(l.isGreaterThan(r)).isFalse();
        assertThat(l.isGreaterThanOrEqualTo(r)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            " ",
            "1",
            "1.2",
            "1.2.3.4",
            "v1.2.3",
            "1.2.x",
            "1..3",
            "a.b.c",
            "1.2.3-",
            "1.2.3-rc#1"
    })
    void invalidVersionsRejected(String invalid) {
        assertThatThrownBy(() -> Version.of(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(invalid.isBlank() ? "must not be blank" : "Invalid workflow version");
        assertThatThrownBy(() -> Version.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateAcceptsLegalVersions() {
        assertThatCode(() -> Version.validate("0.0.1")).doesNotThrowAnyException();
        assertThatCode(() -> Version.validate("10.20.30-rc.1")).doesNotThrowAnyException();
    }

    @Test
    void nullVersionRejected() {
        assertThatThrownBy(() -> Version.validate(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.of(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tryOfReturnsEmptyOnUnparseable() {
        assertThat(Version.tryOf(null)).isEmpty();
        assertThat(Version.tryOf("")).isEmpty();
        assertThat(Version.tryOf("not-a-version")).isEmpty();
        assertThat(Version.tryOf("1.2.3")).contains(Version.of("1.2.3"));
    }

    @Test
    void valueIsCanonicalWireFormat() {
        assertThat(Version.of("1.2.3-rc.1").value()).isEqualTo("1.2.3-rc.1");
        assertThat(Version.of("1.2.3").toString()).isEqualTo("1.2.3");
    }

    @Test
    void closestNotGreaterThanPicksHighestSibling() {
        List<Version> candidates = List.of(
                Version.of("0.0.1"), Version.of("1.0.0"), Version.of("1.5.0"), Version.of("2.0.0"));
        assertThat(Version.closestNotGreaterThan(candidates, Version.of("1.9.0")))
                .contains(Version.of("1.5.0"));
        assertThat(Version.closestNotGreaterThan(candidates, Version.of("1.5.0")))
                .contains(Version.of("1.5.0"));
        assertThat(Version.closestNotGreaterThan(candidates, Version.of("0.0.0")))
                .isEmpty();
    }

    @Test
    void closestHigherThanPicksLowestStrictlyHigherSibling() {
        List<Version> candidates = List.of(
                Version.of("0.0.1"), Version.of("1.0.0"), Version.of("1.5.0"), Version.of("2.0.0"));
        assertThat(Version.closestHigherThan(candidates, Version.of("1.0.0")))
                .contains(Version.of("1.5.0"));
        assertThat(Version.closestHigherThan(candidates, Version.of("2.0.0")))
                .isEmpty();
    }
}
