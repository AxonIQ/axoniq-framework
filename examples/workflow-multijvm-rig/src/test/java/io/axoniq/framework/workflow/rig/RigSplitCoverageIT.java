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
package io.axoniq.framework.workflow.rig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fails when a scenario belongs to neither half of the split.
 * <p>
 * Selecting by tag makes the default failure mode invisible: a new {@code *IT} class with no tag is matched by neither
 * {@code -Dgroups=rig-a} nor {@code -Dgroups=rig-b}, so it never runs, nothing reports it, and both halves stay green.
 * This is the only check in the module that runs in both halves, and it reads the compiled classes rather than the
 * source, so a class that exists but is not selected is exactly what it sees.
 */
@Tag(RigSplit.A)
@Tag(RigSplit.B)
class RigSplitCoverageIT {

    @Test
    void everyScenarioBelongsToExactlyOneHalfOfTheSplit() {
        var byClass = new TreeMap<String, Set<String>>();
        for (var scenario : scenarioClasses()) {
            byClass.put(scenario.getSimpleName(),
                        Arrays.stream(scenario.getAnnotationsByType(Tag.class))
                              .map(Tag::value)
                              .filter(tag -> RigSplit.A.equals(tag) || RigSplit.B.equals(tag))
                              .collect(Collectors.toCollection(TreeSet::new)));
        }
        System.out.printf("EVIDENCE rig-split classes=%d %s%n", byClass.size(), byClass);

        assertThat(byClass).as("no scenario class was found; the split check is not looking where the classes are")
                           .isNotEmpty();
        assertThat(byClass)
                .as("a scenario tagged with neither half is never run by either command, and both halves stay green")
                .allSatisfy((name, tags) -> assertThat(tags)
                        .as("%s must carry exactly one of the split tags", name)
                        .hasSize(RigSplitCoverageIT.class.getSimpleName().equals(name) ? 2 : 1));
    }

    private static List<Class<?>> scenarioClasses() {
        var directory = Path.of(RigSplitCoverageIT.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                            .resolve(RigSplitCoverageIT.class.getPackageName().replace('.', '/'));
        try (var files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString())
                        .filter(name -> name.endsWith("IT.class"))
                        .map(name -> name.substring(0, name.length() - ".class".length()))
                        .<Class<?>>map(name -> loadClass(RigSplitCoverageIT.class.getPackageName() + "." + name))
                        .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not list the compiled scenarios in " + directory, e);
        }
    }

    private static Class<?> loadClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
