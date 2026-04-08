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

package org.axonframework.update.detection;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MachineIdTest {
    private Path tempHome;
    private File idFile;
    private String originalUserHome;

    @BeforeEach
    void setup() throws IOException {
        originalUserHome = System.getProperty("user.home");
        tempHome = Files.createTempDirectory("axon-machineid-test");
        System.setProperty("user.home", tempHome.toString());
        idFile = new File(tempHome.toString() + "/.axoniq/.machine-id");
        if (idFile.exists()) idFile.delete();
    }

    @AfterEach
    void cleanup() throws IOException {
        if (originalUserHome != null) {
            System.setProperty("user.home", originalUserHome);
        } else {
            System.clearProperty("user.home");
        }
        if (idFile.exists()) idFile.delete();
        File parent = idFile.getParentFile();
        if (parent.exists()) parent.delete();
        Files.deleteIfExists(tempHome);
    }

    @Test
    void returnsExistingIdIfFileExists() throws Exception {
        String expectedId = UUID.randomUUID().toString();
        idFile.getParentFile().mkdirs();
        Files.writeString(idFile.toPath(), expectedId);
        MachineId machineId = new MachineId();
        assertEquals(expectedId, machineId.get());
    }

    @Test
    void createsIdFileIfNotExists() throws Exception {
        assertFalse(idFile.exists());
        MachineId machineId = new MachineId();
        String id = machineId.get();
        assertTrue(idFile.exists());
        String fileContent = Files.readString(idFile.toPath());
        assertEquals(id, fileContent);
        // Should be a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(id));
    }
}
