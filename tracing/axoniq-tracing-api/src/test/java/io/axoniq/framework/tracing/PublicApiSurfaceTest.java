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

package io.axoniq.framework.tracing;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Architectural guard enforcing the public API surface of {@code axoniq-tracing-api} (FR-016 / SC-003). The tracing
 * feature deliberately ships a single consolidated {@link SpanFactory}; this test fails the build if any of the
 * forbidden per-component {@code *SpanFactory} interfaces are reintroduced, and pins the set of public types in the
 * base package.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
class PublicApiSurfaceTest {

    private static final String BASE_PACKAGE = "io.axoniq.framework.tracing";

    private static final List<Pattern> FORBIDDEN_NAME_PATTERNS = List.of(
            Pattern.compile(".*BusSpanFactory"),
            Pattern.compile(".*ManagerSpanFactory"),
            Pattern.compile(".*ProcessorSpanFactory"),
            Pattern.compile(".*EmitterSpanFactory"),
            Pattern.compile("RepositorySpanFactory"),
            Pattern.compile("SagaManagerSpanFactory"),
            Pattern.compile("SnapshotterSpanFactory"),
            Pattern.compile("DeadlineManagerSpanFactory")
    );

    private static final Set<String> EXPECTED_PUBLIC_TYPES_IN_BASE_PACKAGE = Set.of(
            "Span",
            "SpanScope",
            "SpanFactory",
            "SpanAttributesProvider",
            "NoOpSpanFactory",
            "MultiSpanFactory",
            "LoggingSpanFactory",
            "SpanNames",
            "ProcessingContextSpanBinding",
            "MetadataContextPropagator"
    );

    private final JavaClasses tracingClasses = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE_PACKAGE);

    @Test
    void noForbiddenPerComponentSpanFactoryIsExposed() {
        List<String> offenders = tracingClasses.stream()
                                               .filter(this::isPublic)
                                               .map(JavaClass::getSimpleName)
                                               .filter(this::matchesForbiddenPattern)
                                               .collect(Collectors.toList());

        assertThat(offenders)
                .withFailMessage("Forbidden per-component *SpanFactory types found: %s. The feature ships a single "
                                         + "consolidated SpanFactory (FR-016/SC-003).", offenders)
                .isEmpty();
    }

    @Test
    void basePackageExposesExactlyTheExpectedPublicTypes() {
        Set<String> actual = tracingClasses.stream()
                                           .filter(JavaClass::isTopLevelClass)
                                           .filter(this::isPublic)
                                           .filter(clazz -> clazz.getPackageName().equals(BASE_PACKAGE))
                                           .map(JavaClass::getSimpleName)
                                           .filter(name -> !name.equals("package-info"))
                                           .collect(Collectors.toSet());

        assertThat(actual).containsExactlyInAnyOrderElementsOf(EXPECTED_PUBLIC_TYPES_IN_BASE_PACKAGE);
    }

    private boolean isPublic(JavaClass clazz) {
        return clazz.getModifiers().contains(JavaModifier.PUBLIC);
    }

    private boolean matchesForbiddenPattern(String simpleName) {
        return FORBIDDEN_NAME_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(simpleName).matches());
    }
}
