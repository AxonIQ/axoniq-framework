/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import com.tngtech.archunit.junit.AnalyzeClasses;
import org.axonframework.common.archunit.MainArchUnitConventions;

@AnalyzeClasses(packages = ArchUnitPackageRulesTest.BASE_PACKAGE_NAME, importOptions = DoNotIncludeTests.class)
class ArchitectureTest implements MainArchUnitConventions {

}
