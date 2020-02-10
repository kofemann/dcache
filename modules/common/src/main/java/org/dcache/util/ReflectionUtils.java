package org.dcache.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains useful static methods for working with Java
 * reflection.
 */
public class ReflectionUtils
{
    private static final Map<String,MethodHandle> methodCache =
        new HashMap<>();

    /**
     * Finds a maximally specific public method called <i>name</i> in
     * <i>c</i> accepting parameters of type <i>parameters</i>.
     *
     * In contrast to <code>Class.getMethod</code>,
     * <code>resolve</code> performs type widening on the parameters,
     * in effect emulating the steps performed at compile time for
     * finding the a method.
     *
     * Notice that we return the first method found in a depth-first
     * left-to-right search. This is different from what Java does at
     * compile time. We do not support auto-boxing or methods with a
     * variable number of arguments. Lack of auto-boxing means the
     * methods with parameters of primitive types are never returned.
     *
     * To improve performance, a cache of resolved methods is
     * maintained.
     *
     * @returns a matching method or null if no method is found
     */
    public static MethodHandle resolve(Class<?> c, String name, Class<?> ... parameters)
    {
        try {
            Object[] signature = {c, name, parameters};
            String key = Arrays.deepToString(signature);

            /* Cache lookup.
             */
            MethodHandle m = methodCache.get(key);
            if (m != null) {
                return m;
            }

            /* Lookup in class c.
             */
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            Method mm = c.getMethod(name, parameters);

            m = lookup.unreflect(mm);
            methodCache.put(key, m);
            return m;
        } catch (IllegalAccessException e) {
            return null;
        } catch (NoSuchMethodException e) {
            /* Perform type widening on parameters to find a matching
             * method.
             */
            for (int i = 0; i < parameters.length; i++) {
                Class<?> s = parameters[i].getSuperclass();
                if (s != null) {
                    Class<?> old = parameters[i];
                    parameters[i] = s;
                    MethodHandle m = resolve(c, name, parameters);
                    if (m != null) {
                        return m;
                    }
                    parameters[i] = old;
                }
            }

            /* We cannot find a matching method, give up.
             */
            return null;
        }
    }

    public static boolean hasDeclaredException(Method method, Exception exception)
    {
        for (Class<?> clazz: method.getExceptionTypes()) {
            if (clazz.isAssignableFrom(exception.getClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Like Class#getMethod, but also returns non-public methods. Differs from
     * Class#getDeclaredMethod by also searching super classes.
     */
    public static Method getAnyMethod(Class<?> clazz, String name, Class<?>... parameterTypes) throws NoSuchMethodException
    {
        try {
            // Because execute is protected, we cannot use getMethod.
            return clazz.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            Class<?> superclass = clazz.getSuperclass();
            if (superclass != null) {
                return getAnyMethod(superclass, name, parameterTypes);
            }
            throw e;
        }
    }
}
