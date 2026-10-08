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

package org.axonframework.modelling.saga.repository;

import com.thoughtworks.xstream.XStream;
import org.assertj.core.api.Assertions;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;

/**
 * Runs reflective actions through the child-first class loader that keeps the Axon Framework 4 jars' classes out of
 * the reactor's Axon Framework 5 classes, and builds a {@link XStream} instance configured exactly as Axon
 * Framework 4's {@code XStreamSerializer} would inside it. Shared by tests that compare Axon Framework 4-produced
 * serialized forms against their Axon Framework 5 {@code Converter} counterparts.
 *
 * @author Steven van Beelen
 */
public final class Af4ClassLoaderSupport {

    private static final Path AF4_MESSAGING_JAR = Path.of("target", "af4-serializer", "axon-messaging-af4.jar");
    private static final Path AF4_MODELLING_JAR = Path.of("target", "af4-serializer", "axon-modelling-af4.jar");

    private Af4ClassLoaderSupport() {
        // Utility class
    }

    /**
     * Runs the given {@code action} with a class loader that resolves Axon Framework 4 classes from the jars copied by
     * this module's {@code copy-af4-serializer} build step, closing it once {@code action} returns.
     *
     * @param action the action to run with the Axon Framework 4 class loader
     * @param <T>    the type of result produced by {@code action}
     * @return the result of {@code action}
     * @throws Exception whatever reflective or I/O exception {@code action} throws
     */
    public static <T> T withAf4ClassLoader(Af4Action<T> action) throws Exception {
        Assertions.assertThat(AF4_MESSAGING_JAR).isRegularFile();
        Assertions.assertThat(AF4_MODELLING_JAR).isRegularFile();
        URL messagingJar = AF4_MESSAGING_JAR.toUri().toURL();
        URL modellingJar = AF4_MODELLING_JAR.toUri().toURL();
        try (AxonFramework4ClassLoader classLoader = new AxonFramework4ClassLoader(messagingJar, modellingJar)) {
            return action.run(classLoader);
        }
    }

    /**
     * Builds an Axon Framework 4 {@code XStreamSerializer} through the given {@code classLoader} and returns the
     * {@link XStream} instance it configured, with that same {@code classLoader} set on it.
     * <p>
     * Axon Framework 4's alias resolution rewrites an aliased element name to the type's fully qualified name and
     * resolves that through the {@code XStream} instance's own class loader. Without this, that class loader would stay
     * the one active when {@code new XStream()} below ran, rather than the one the Axon Framework 4 classes were loaded
     * from, and resolution would fail even for Axon Framework 4's own types.
     *
     * @param classLoader the class loader to build the Axon Framework 4 {@code XStreamSerializer} through
     * @return the {@link XStream} instance used by the Axon Framework 4 {@code XStreamSerializer}
     * @throws Exception whatever reflective exception the Axon Framework 4 {@code XStreamSerializer.Builder} throws
     */
    public static XStream af4XStream(ClassLoader classLoader) throws Exception {
        Class<?> serializerType = classLoader.loadClass("org.axonframework.serialization.xml.XStreamSerializer");
        Object builder = serializerType.getMethod("builder").invoke(null);
        builder.getClass().getMethod("xStream", XStream.class).invoke(builder, new XStream());
        builder.getClass().getMethod("classLoader", ClassLoader.class).invoke(builder, classLoader);
        Object serializer = builder.getClass().getMethod("build").invoke(builder);
        return (XStream) serializer.getClass().getMethod("getXStream").invoke(serializer);
    }

    /**
     * Constructs an instance of the given Axon Framework 4 {@code className} through the given {@code classLoader},
     * using its {@code (String, Object)} constructor.
     *
     * @param classLoader the class loader to load {@code className} through
     * @param className   the fully qualified name of the Axon Framework 4 class to construct
     * @param type        the first constructor argument
     * @param identifier  the second constructor argument
     * @return the constructed instance
     * @throws Exception whatever reflective exception the constructor call throws
     */
    public static Object construct(ClassLoader classLoader, String className, String type, Object identifier)
            throws Exception {
        Class<?> loadedType = classLoader.loadClass(className);
        Assertions.assertThat(loadedType.getClassLoader())
                  .as("[%s] must resolve from the Axon Framework 4 class loader, not fall back to the parent "
                              + "class loader's Axon Framework 5 class of the same name", className)
                  .isEqualTo(classLoader);
        return loadedType.getConstructor(String.class, Object.class).newInstance(type, identifier);
    }

    /**
     * An action run with an Axon Framework 4 class loader by {@link #withAf4ClassLoader(Af4Action)}.
     *
     * @param <T> the type of result produced by this action
     */
    public interface Af4Action<T> {

        /**
         * Runs this action with the given {@code classLoader}.
         *
         * @param classLoader the class loader to run this action with
         * @return the result of this action
         * @throws Exception whatever reflective or I/O exception this action throws
         */
        T run(ClassLoader classLoader) throws Exception;
    }

    /**
     * Loads {@code org.axonframework.**} classes from the Axon Framework 4 jars first, falling back to the parent
     * class loader for anything they do not contain, such as Axon Framework 5 test fixtures under the same namespace.
     */
    private static final class AxonFramework4ClassLoader extends URLClassLoader {

        private AxonFramework4ClassLoader(URL... af4Jars) {
            super(af4Jars, Af4ClassLoaderSupport.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null && name.startsWith("org.axonframework.")) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                        // Not shipped in the Axon Framework 4 jar; fall through to the parent class loader below.
                    }
                }
                if (loadedClass == null) {
                    loadedClass = super.loadClass(name, false);
                }
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        }
    }
}
