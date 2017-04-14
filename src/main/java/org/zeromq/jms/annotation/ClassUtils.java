package org.zeromq.jms.annotation;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to assist in finding classes with a specified annotation.
 */
public class ClassUtils {
    private static final Logger LOGGER = Logger.getLogger(ClassUtils.class.getCanonicalName());

    /**
     * Stop construction of utility class.
     */
    private ClassUtils() {
    };

    /**
     * Return the class from a list of classes that contain the specified annotation attribute and value. Duplicates
     * will cause an IllegalstateException. If non can be found the class short name will be check a possible name.
     * @param possibleClasses              the list of possible class
     * @param annotation                   the annotation class
     * @param name                         the annotation method name to check when annotation exists
     * @param value                        the query value too match against annotation method value or class name
     * @return                             return the found class, or null when not found
     * @throws IllegalAccessException      can throw illegal access exception
     * @throws IllegalArgumentException    can throw illegal argument exception
     * @throws InvocationTargetException   can throw invocation target exception
     */
    public static Class<?> getClass(final List<Class<?>> possibleClasses, final Class<? extends Annotation> annotation, final String name,
            final String value) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Class<?> clazz = null;

        for (Class<?> possibleClass : possibleClasses) {
            if (possibleClass.isAnnotationPresent(annotation)) {
                final Annotation annotationClass = possibleClass.getAnnotation(annotation);
                final Class<? extends Annotation> type = annotationClass.annotationType();

                for (Method method : type.getDeclaredMethods()) {
                    if (method.getName().equals(name) && method.getParameterTypes().length == 0) {
                        Object attributeValue = method.invoke(annotationClass);

                        if (attributeValue != null && attributeValue.equals(value)) {
                            if (clazz != null) {
                                throw new IllegalStateException("Conflict with value (" + name + "=" + value + " ) between classes: "
                                        + clazz.getCanonicalName() + ", " + possibleClass.getCanonicalName());
                            }

                            clazz = possibleClass;
                        }
                    }
                }
            }

            if (possibleClass.getSimpleName().equals(value)) {
                if (clazz != null) {
                    throw new IllegalStateException("Conflict with value (" + name + "=" + value + " ) between classes: " + clazz.getCanonicalName()
                            + ", " + possibleClass.getCanonicalName());
                }

                clazz = possibleClass;
            }
        }

        // One last try, may been value is a class?
        if (clazz == null && value.contains(".")) {
            // CHECKSTYLE:OFF: Empty Block
            try {
                clazz = Class.forName(value);
            } catch (ClassNotFoundException ex) {
            }
            // CHECKSTYLE:ON: Empty Block
        }
        return clazz;
    }

    /**
     * Return the setter method using the specified annotation attribute and value. Duplicates
     * will cause an IllegalstateException. If non can be found the method name will be check as
     * possible match.
     * @param clazz                        the class to investigate
     * @param annotation                   the annotation class
     * @param name                         the annotation method name to check when annotation exists
     * @param value                        the query value too match against annotation method value or class name
     * @return                             return the found method, or null when not found
     * @throws IllegalAccessException      can throw illegal access exception
     * @throws IllegalArgumentException    can throw illegal argument exception
     * @throws InvocationTargetException   can throw invocation target exception
     */
    public static Method getSetterMethod(final Class<?> clazz, final Class<? extends Annotation> annotation, final String name, final String value)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method setterMethod = null;

        for (Method possibleMethod : clazz.getMethods()) {
            if (possibleMethod.isAnnotationPresent(annotation)) {
                Annotation[] methodAnnotations = possibleMethod.getDeclaredAnnotations();

                for (Annotation methodAnnotation : methodAnnotations) {
                    if (annotation.isInstance(methodAnnotation)) {
                        final Class<? extends Annotation> type = methodAnnotation.annotationType();

                        for (Method annotationMethod : type.getDeclaredMethods()) {
                            if (annotationMethod.getName().equals(name) && annotationMethod.getParameterTypes().length == 0) {
                                Object attributeValue = annotationMethod.invoke(methodAnnotation);

                                if (attributeValue != null && attributeValue.equals(value)) {
                                    if (setterMethod != null) {
                                        throw new IllegalStateException("Conflict with value (" + name + "=" + value + " ) between methods: "
                                                + setterMethod.getName() + ", " + possibleMethod.getName());
                                    }

                                    setterMethod = possibleMethod;
                                }
                            }
                        }
                    }

                }
            }

            // check name is not the same
            if (possibleMethod.getParameterTypes().length == 1) {
                final String methodName = possibleMethod.getName();

                if (methodName.equalsIgnoreCase(value) || methodName.equalsIgnoreCase("set" + value)) {
                    if (setterMethod != null) {
                        throw new IllegalStateException("Conflict with value (" + name + "=" + value + " ) between methods: "
                                + setterMethod.getName() + ", " + possibleMethod.getName());
                    }

                    setterMethod = possibleMethod;
                }
            }
        }

        return setterMethod;
    }

    /**
     * Return all method(s) of a specified class that contain the specified annotation.
     * @param clazz                    the class to interrogate
     * @param annotation               the annotation to find
     * @return                         return the list of methods found (an empty list is possible)
     */
    public static List<Method> getMethods(final Class<?> clazz, final Class<? extends Annotation> annotation) {
        final List<Method> annotatedMethods = new LinkedList<Method>();

        for (Method method : clazz.getMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                annotatedMethods.add(method);
            }
        }

        return annotatedMethods;
    }

    /**
     * Return all classes below the package root that have the specified annotation.
     * @param packageName              the starting package to search.
     * @param annotation               the annotation to find
     * @return                         return a list of classes found (an empty list is possible)
     * @throws ClassNotFoundException  throws class not found
     * @throws IOException             throws I/O exception in searching package structures
     */
    public static List<Class<?>> getClasses(final String packageName, final Class<? extends Annotation> annotation) throws ClassNotFoundException,
            IOException {
        final List<Class<?>> classes = getClasses(packageName);
        final List<Class<?>> annotatedClasses = new LinkedList<Class<?>>();

        for (Class<?> clazz : classes) {
            if (clazz.isAnnotationPresent(annotation)) {
                annotatedClasses.add(clazz);
            }
        }

        return annotatedClasses;
    }

    /**
     * Check weather a package within the class path and that it can be read.
     * @param packageName  the name of the package
     * @return             return true of existing, otherwise false
     */
    public static boolean packageExists(final String packageName) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final String path = packageName.replace('.', '/');

        try {
            Enumeration<URL> resources = classLoader.getResources(path);
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();

                if (resource == null) {
                    LOGGER.warning("Unable to retrieve resources from the package: " + packageName);

                    return false;
                }
            }
        } catch (IOException ex) {
            LOGGER.warning("Unable to locate package: " + packageName);

            return false;
        }

        return true;
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and sub-packages.
     * @param basePackageName          the base package
     * @return                         return the classes foundThe classes
     * @throws ClassNotFoundException  throws class not found exception
     * @throws IOException             throws I/O exception
     */
    public static List<Class<?>> getClasses(final String basePackageName) throws ClassNotFoundException, IOException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final String path = basePackageName.replace('.', '/');
        final Enumeration<URL> resources = classLoader.getResources(path);
        final Set<File> dirs = new HashSet<File>();

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }

        final List<Class<?>> classes = new LinkedList<Class<?>>();

        for (File directory : dirs) {
            classes.addAll(findClasses(directory, basePackageName));
        }

        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and sub-directories.
     * @param directory                the base directory
     * @param packageName              the package name for classes found inside the base directory
     * @return                         return the found classes
     * @throws ClassNotFoundException  throws class not found exception
     */
    private static List<Class<?>> findClasses(final File directory, final String packageName) throws ClassNotFoundException {
        final List<Class<?>> classes = new LinkedList<Class<?>>();

        if (!directory.exists()) {
            return classes;
        }

        final File[] files = directory.listFiles();

        for (File file : files) {
            if (file.isDirectory()) {
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                try {
                    classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
                } catch (UnsupportedClassVersionError ex) {
                    LOGGER.log(Level.WARNING, "Unable to loader class: " + file.getName(), ex);
                }
            }
        }

        return classes;
    }

    /**
     * Set the values of any matching parameters to class setter values, that have URI parameter.
     * @param  parameters                    the map of name/values pairs
     * @param  config                        the instantiated class
     * @throws ReflectiveOperationException  throws reflective operation exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void setMethods(final Map<String, List<String>> parameters, final Object config) throws ReflectiveOperationException {
        final List<Method> methods = getMethods(config.getClass(), ZmqUriParameter.class);

        for (Method method : methods) {
            final ZmqUriParameter attribute = (ZmqUriParameter) method.getAnnotation(ZmqUriParameter.class);
            final String paramName = attribute.value();
            final List<String> paramValues = parameters.get(paramName);
            final String paramValue = (paramValues == null || paramValues.size() == 0) ? null : paramValues.get(0);

            // Found a valid value to set
            if (paramValue != null) {
                Object value = null;
                final Class<?>[] paramTypes = method.getParameterTypes();
                // check for at setter value only
                if (paramTypes != null && paramTypes.length == 1) {
                    final Class<?> paramType = paramTypes[0];

                    if (paramType == int.class || paramType.isAssignableFrom(Integer.class)) {
                        value = Integer.parseInt(paramValue);
                    } else if (paramType == long.class || paramType.isAssignableFrom(Long.class)) {
                        value = Long.parseLong(paramValue);
                    } else if (paramType == short.class || paramType.isAssignableFrom(Short.class)) {
                        value = Short.decode(paramValue);
                    } else if (paramType == double.class || paramType.isAssignableFrom(Double.class)) {
                        value = Double.parseDouble(paramValue);
                    } else if (paramType == float.class || paramType.isAssignableFrom(Float.class)) {
                        value = Float.parseFloat(paramValue);
                    } else if (paramType == boolean.class || paramType.isAssignableFrom(Boolean.class)) {
                        value = Boolean.parseBoolean(paramValue);
                    } else if (paramType.isAssignableFrom(byte[].class)) {
                        value = paramValue.getBytes();
                    } else if (paramType.isAssignableFrom(String.class)) {
                        value = paramValue;
                    } else if (paramType.isEnum()) {
                        value = Enum.valueOf((Class<Enum>) paramType, paramValue);
                    } else {
                        throw new UnsupportedOperationException("Unable to map parameter [" + paramName + "] with value ["
                            + paramValue + "] to required type: " + paramType);
                    }
                }

                if (value != null) {
                    try {
                        LOGGER.info("Setting configuration parameter " + paramName + "=" + paramValue + " on: " + config);
                        method.invoke(config, value);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                        LOGGER.log(Level.SEVERE, "Unable to invoke setter method: " + method.getName() + "(" + value + ")", ex);

                        throw ex;
                    }
                }
            }
        }
    }

}
