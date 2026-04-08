package io.axoniq.workflow;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.DependencyRules;
import com.tngtech.archunit.library.freeze.FreezingArchRule;

import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

@AnalyzeClasses(
        packages = {"io.axoniq.workflow"},
        importOptions = {ImportOption.DoNotIncludeTests.class}
)
public class WorkflowArchUnitConventionTests
        // TODO#115 as part of the implementation of https://github.com/AxonIQ/extension-workflow/issues/115 uncomment the following line
//        implements MainArchUnitConventions
{


    @ArchTest
    private final ArchRule packagesShouldBeFreeOfCycles =
            FreezingArchRule.freeze(
                    slices()
                            .matching("(**)")
                            .should()
                            .beFreeOfCycles()
                            .as("Package Cycles")
            );

    @ArchTest
    private final ArchRule noClassesShouldDependOnUpperPackages =
            DependencyRules.NO_CLASSES_SHOULD_DEPEND_UPPER_PACKAGES
                    .as("Package Hierarchy Violations");
}
