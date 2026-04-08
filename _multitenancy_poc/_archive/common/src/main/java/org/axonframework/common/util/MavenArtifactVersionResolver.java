/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.axonframework.common.io.IOUtils.closeQuietly;

/**
 * Artifact version resolver that uses Maven metadata to retrieve the jar version.
 * <p/>
 * By default, Maven stores the meta-data in a file called 'pom.properties' in the JAR files under
 * 'META-INF/maven/&lt;groupId&gt;/&lt;artifactId&gt;/'.
 *
 * @author Sara Pellegrini
 * @since 4.2.1
 */
public class MavenArtifactVersionResolver {

    private final String groupId;

    private final String artifactId;

    private final ClassLoader classLoader;

    /**
     * Creates an instance to look for the version in the metadata of the artifact with given
     * {@code groupId} and {@code artifactId}.
     *
     * @param groupId     The groupId as defined in the pom.xml file of the module
     * @param artifactId  The artifactId as defined in the pom.xml file of the module
     * @param classLoader The class loader to load the artifact configuration with
     */
    public MavenArtifactVersionResolver(String groupId, String artifactId, ClassLoader classLoader) {
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.classLoader = classLoader;
    }

    /**
     * Returns the specified jar version.
     *
     * @return the version in the metadata of the artifact
     * @throws IOException When an exception occurs reading from the maven configuration file
     */
    public String get() throws IOException {

        final InputStream propFile = classLoader.getResourceAsStream(
                "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties");

        if (propFile != null) {
            try {
                Properties mavenProps = new Properties();
                mavenProps.load(propFile);
                return mavenProps.getProperty("version");
            } finally {
                closeQuietly(propFile);
            }
        } else {
            return null;
        }
    }
}
