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

package org.axonframework.deadline;

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gives tests access to the classes of Axon Framework 4.13.2, loaded in an isolated, child-first class loader from the
 * jars the build copies to {@code target/af4-serializer}. Classes outside {@code org.axonframework}, such as the
 * scheduler libraries and the tests' payload classes, come from the test class path, so that both versions share them.
 * <p>
 * Axon Framework 4 objects are handled through reflection, as their classes share names with the Axon Framework 5
 * classes of the test class path.
 *
 * @author Jakob Hatzl
 */
public final class AxonFramework4 implements AutoCloseable {

    private static final Path AF4_JARS = Path.of("target", "af4-serializer");

    private final URLClassLoader classLoader;

    /**
     * Opens the class loader over the Axon Framework 4 messaging and modelling jars.
     *
     * @throws IOException if the jars cannot be found
     */
    public AxonFramework4() throws IOException {
        Path messaging = AF4_JARS.resolve("axon-messaging-af4.jar");
        Path modelling = AF4_JARS.resolve("axon-modelling-af4.jar");
        assertThat(messaging).isRegularFile();
        assertThat(modelling).isRegularFile();
        this.classLoader = new ChildFirstClassLoader(
                new URL[]{messaging.toUri().toURL(), modelling.toUri().toURL()}
        );
    }

    /**
     * Returns the Axon Framework 4 class of the given name.
     *
     * @param className the fully qualified class name
     * @return the Axon Framework 4 class
     */
    public Class<?> type(String className) {
        try {
            return Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates an instance of the given Axon Framework 4 class through the constructor taking the given arguments.
     *
     * @param className the fully qualified class name
     * @param arguments the constructor arguments
     * @return the new instance
     */
    public Object create(String className, Object... arguments) {
        Class<?> type = type(className);
        return Arrays.stream(type.getDeclaredConstructors())
                     .filter(constructor -> constructor.getParameterCount() == arguments.length)
                     .filter(constructor -> accepts(constructor.getParameterTypes(), arguments))
                     .findFirst()
                     .map(constructor -> {
                         Thread thread = Thread.currentThread();
                         ClassLoader original = thread.getContextClassLoader();
                         thread.setContextClassLoader(classLoader);
                         try {
                             constructor.setAccessible(true);
                             return constructor.newInstance(arguments);
                         } catch (ReflectiveOperationException e) {
                             throw new IllegalStateException(rootCause(e));
                         } finally {
                             thread.setContextClassLoader(original);
                         }
                     })
                     .orElseThrow(() -> new IllegalArgumentException("No matching constructor on " + className));
    }

    /**
     * Invokes the static method of the given name on the given Axon Framework 4 class.
     *
     * @param className  the fully qualified class name
     * @param methodName the name of the static method
     * @param arguments  the method arguments
     * @return the result of the invocation
     */
    public Object callStatic(String className, String methodName, Object... arguments) {
        return invoke(type(className), null, methodName, arguments);
    }

    /**
     * Invokes the method of the given name on the given Axon Framework 4 object.
     *
     * @param target     the object to invoke the method on
     * @param methodName the name of the method
     * @param arguments  the method arguments
     * @return the result of the invocation
     */
    public Object call(Object target, String methodName, Object... arguments) {
        return invoke(target.getClass(), target, methodName, arguments);
    }

    /**
     * Creates Axon Framework 4's {@code JacksonSerializer} with its defaults.
     *
     * @return the serializer
     */
    public Object jacksonSerializer() {
        return callStatic("org.axonframework.serialization.json.JacksonSerializer", "defaultSerializer");
    }

    /**
     * Creates Axon Framework 4's {@code Jackson3Serializer}, with its defaults or with default typing.
     *
     * @param defaultTyping whether to activate the serializer's default typing
     * @return the serializer
     */
    public Object jackson3Serializer(boolean defaultTyping) {
        String className = "org.axonframework.serialization.jackson3.Jackson3Serializer";
        if (!defaultTyping) {
            return callStatic(className, "defaultSerializer");
        }
        return call(call(callStatic(className, "builder"), "defaultTyping"), "build");
    }

    /**
     * Creates an Axon Framework 4 {@code GenericDeadlineMessage}.
     *
     * @param deadlineName the name of the deadline
     * @param payload      the payload, from the test class path
     * @param metadata     the metadata, which may hold values of any type
     * @return the deadline message
     */
    public Object deadlineMessage(String deadlineName, Object payload, Map<String, ?> metadata) {
        return create("org.axonframework.deadline.GenericDeadlineMessage", deadlineName, payload, metadata);
    }

    /**
     * Serializes the given object with the given Axon Framework 4 serializer.
     *
     * @param serializer     the Axon Framework 4 serializer
     * @param object         the object to serialize
     * @param representation the representation to serialize to
     * @return the serialized data
     */
    public Object serialize(Object serializer, Object object, Class<?> representation) {
        return call(call(serializer, "serialize", object, representation), "getData");
    }

    /**
     * Writes the given object with Java serialization.
     *
     * @param object the object to write
     * @return the written bytes
     */
    public static byte[] javaSerialize(Object object) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(object);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bytes.toByteArray();
    }

    /**
     * Reads the given bytes with Java serialization, resolving classes from the test class path, as Axon Framework 5
     * does.
     *
     * @param bytes the bytes to read
     * @return the read object
     */
    public static Object javaDeserializeAsAxonFramework5(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Reads the given bytes with Java serialization, resolving classes from Axon Framework 4, as an Axon Framework 4
     * node does.
     *
     * @param bytes the bytes to read
     * @return the read object
     */
    public Object javaDeserializeAsAxonFramework4(byte[] bytes) {
        try (ObjectInputStream in = new Axon4ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates an Axon Framework 4 {@code SagaScopeDescriptor}.
     *
     * @param type       the type of the Saga
     * @param identifier the identifier of the Saga
     * @return the scope descriptor
     */
    public Object sagaScope(String type, Object identifier) {
        return create("org.axonframework.modelling.saga.SagaScopeDescriptor", type, identifier);
    }

    /**
     * Creates an Axon Framework 4 {@code AggregateScopeDescriptor}.
     *
     * @param type       the type of the aggregate
     * @param identifier the identifier of the aggregate
     * @return the scope descriptor
     */
    public Object aggregateScope(String type, Object identifier) {
        return create("org.axonframework.modelling.command.AggregateScopeDescriptor", type, identifier);
    }

    /**
     * Deserializes the given data, stored under the given type name, with the given Axon Framework 4 serializer.
     *
     * @param serializer the Axon Framework 4 serializer
     * @param data       the serialized data
     * @param typeName   the stored type name
     * @return the deserialized object
     */
    public Object deserialize(Object serializer, Object data, String typeName) {
        Object serializedObject = create("org.axonframework.serialization.SimpleSerializedObject",
                                         data, data.getClass(), typeName, null);
        return call(serializer, "deserialize", serializedObject);
    }

    @Override
    public void close() throws IOException {
        classLoader.close();
    }

    /**
     * The serializers an Axon Framework 4 deadline manager could store deadlines with, each with the Axon Framework 5
     * {@link Converter} that reads and writes the same form.
     */
    public enum Flavor {

        /**
         * Axon Framework 4's {@code JacksonSerializer} with its defaults, and a default {@link JacksonConverter}.
         */
        JACKSON {
            @Override
            public Object axonFramework4Serializer(AxonFramework4 axonFramework4) {
                return axonFramework4.jacksonSerializer();
            }

            @Override
            public Converter axonFramework5Converter() {
                return new JacksonConverter();
            }
        },
        /**
         * Axon Framework 4's {@code Jackson3Serializer} with its defaults, and a default {@link JacksonConverter}.
         */
        JACKSON3 {
            @Override
            public Object axonFramework4Serializer(AxonFramework4 axonFramework4) {
                return axonFramework4.jackson3Serializer(false);
            }

            @Override
            public Converter axonFramework5Converter() {
                return new JacksonConverter();
            }
        },
        /**
         * Axon Framework 4's {@code Jackson3Serializer} with default typing, and a {@link JacksonConverter} whose
         * mapper activates the same default typing.
         */
        JACKSON3_DEFAULT_TYPING {
            @Override
            public Object axonFramework4Serializer(AxonFramework4 axonFramework4) {
                return axonFramework4.jackson3Serializer(true);
            }

            @Override
            public Converter axonFramework5Converter() {
                PolymorphicTypeValidator typeValidator =
                        BasicPolymorphicTypeValidator.builder().allowIfSubType(Object.class).build();
                return new JacksonConverter(
                        JsonMapper.builder()
                                  .activateDefaultTyping(typeValidator, DefaultTyping.NON_CONCRETE_AND_ARRAYS)
                                  .polymorphicTypeValidator(typeValidator)
                                  .build()
                );
            }
        };

        /**
         * Creates the Axon Framework 4 serializer of this flavor.
         *
         * @param axonFramework4 the access to Axon Framework 4's classes
         * @return the Axon Framework 4 serializer
         */
        public abstract Object axonFramework4Serializer(AxonFramework4 axonFramework4);

        /**
         * Creates the Axon Framework 5 converter of this flavor.
         *
         * @return the Axon Framework 5 converter
         */
        public abstract Converter axonFramework5Converter();
    }

    /**
     * A deadline payload both versions convert, nested in another class, as a stored type name keeps the enclosing
     * class.
     */
    public static final class Payloads {

        private Payloads() {
        }

        /**
         * A deadline payload with a text, a number and an instant.
         */
        public static final class CompatPayload {

            private String text;
            private int number;
            private Instant when;

            /**
             * Creates an empty payload, for deserialization.
             */
            public CompatPayload() {
            }

            /**
             * Creates a payload with the given values.
             *
             * @param text   a text value
             * @param number a number value
             * @param when   an instant
             */
            public CompatPayload(String text, int number, Instant when) {
                this.text = text;
                this.number = number;
                this.when = when;
            }

            public String getText() {
                return text;
            }

            public void setText(String text) {
                this.text = text;
            }

            public int getNumber() {
                return number;
            }

            public void setNumber(int number) {
                this.number = number;
            }

            public Instant getWhen() {
                return when;
            }

            public void setWhen(Instant when) {
                this.when = when;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof CompatPayload that
                        && number == that.number
                        && Objects.equals(text, that.text)
                        && Objects.equals(when, that.when);
            }

            @Override
            public int hashCode() {
                return Objects.hash(text, number, when);
            }

            @Override
            public String toString() {
                return "CompatPayload{text='" + text + "', number=" + number + ", when=" + when + '}';
            }
        }
    }

    private Object invoke(Class<?> type, Object target, String methodName, Object... arguments) {
        // Axon Framework 4 resolves classes, such as its own MetaData, through the context class loader.
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(classLoader);
        try {
            return invokeMethod(type, target, methodName, arguments);
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    private static Object invokeMethod(Class<?> type, Object target, String methodName, Object... arguments) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.getName().equals(methodName)
                        && method.getParameterCount() == arguments.length
                        && accepts(method.getParameterTypes(), arguments)) {
                    try {
                        method.setAccessible(true);
                        return method.invoke(target, arguments);
                    } catch (InvocationTargetException e) {
                        throw rethrow(e.getCause());
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
        for (Class<?> anInterface : type.getInterfaces()) {
            for (Method method : anInterface.getMethods()) {
                if (method.getName().equals(methodName) && method.getParameterCount() == arguments.length) {
                    try {
                        return method.invoke(target, arguments);
                    } catch (InvocationTargetException e) {
                        throw rethrow(e.getCause());
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
        throw new IllegalArgumentException("No method " + methodName + " on " + type.getName());
    }

    private static boolean accepts(Class<?>[] parameterTypes, Object[] arguments) {
        for (int i = 0; i < parameterTypes.length; i++) {
            if (arguments[i] != null && !wrap(parameterTypes[i]).isInstance(arguments[i])) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> wrap(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        return type == boolean.class ? Boolean.class : type == int.class ? Integer.class
                : type == long.class ? Long.class : Object.class;
    }

    private static Throwable rootCause(Throwable throwable) {
        return throwable instanceof InvocationTargetException ite ? ite.getCause() : throwable;
    }

    private static RuntimeException rethrow(Throwable cause) {
        return cause instanceof RuntimeException runtime ? runtime : new IllegalStateException(cause);
    }

    private final class Axon4ObjectInputStream extends ObjectInputStream {

        private Axon4ObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass description) throws IOException, ClassNotFoundException {
            return Class.forName(description.getName(), false, classLoader);
        }
    }

    private static final class ChildFirstClassLoader extends URLClassLoader {

        private ChildFirstClassLoader(URL[] jars) {
            super(jars, AxonFramework4.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null && name.startsWith("org.axonframework.")) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                        // Test classes, such as payloads, are shared through the parent class loader.
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
