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

package io.axoniq.framework.messaging.eventstreaming;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.DependencyRules;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;

import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

@AnalyzeClasses(packages = ArchUnitPackageRulesTest.BASE_PACKAGE_NAME)
public class ArchUnitPackageRulesTest {

    public static final String BASE_PACKAGE_NAME = "io.axoniq.framework.messaging.eventstreaming";

    @ArchTest
    private final ArchRule packagesShouldBeFreeOfCycles = slices()
            .matching("(**)")
            .should()
            .beFreeOfCycles()
            .as("Package Cycles");

    @ArchTest
    private final ArchRule noClassesShouldDependOnUpperPackages = DependencyRules
            .NO_CLASSES_SHOULD_DEPEND_UPPER_PACKAGES
            .as("Package Hierarchy Violations");

    @Test
    void shouldMatchPackageName() {
        assertThat(MethodHandles.lookup().lookupClass().getPackageName()).isEqualTo(BASE_PACKAGE_NAME);
    }
}
