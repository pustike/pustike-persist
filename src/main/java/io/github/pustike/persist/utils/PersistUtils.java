/*
 * Copyright (C) 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.pustike.persist.utils;

import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Common Utilities.
 */
public final class PersistUtils {
    private static final Map<Class<?>, Class<?>> primitiveToWrapperMap = Map.of(boolean.class, Boolean.class,
        byte.class, Byte.class, char.class, Character.class, double.class, Double.class, float.class, Float.class,
        int.class, Integer.class, long.class, Long.class, short.class, Short.class, void.class, Void.class);
    private static final char[] lowerCaseLetters = new char[26];

    static {
        // 97 - 122 are lower-case chars
        IntStream.range(0, 26).forEach(i -> lowerCaseLetters[i] = (char) (i + 97));
    }

    private PersistUtils() {

    }

    /**
     * Get the corresponding wrapper class for the given primitive class.
     * @param clazz the primitive class
     * @return the corresponding wrapper class if primitive, else the same input parameter
     */
    public static Class<?> getWrapperClass(Class<?> clazz) {
        Objects.requireNonNull(clazz);
        return clazz.isPrimitive() ? primitiveToWrapperMap.get(clazz) : clazz;
    }

    /**
     * Generate a string for the given hashCode by mapping it to character codes.
     * @param hashCode the hash code
     * @return a string of characters for the hash code
     */
    public static String hashCodeToString(int hashCode) {
        StringBuilder builder = new StringBuilder();
        hashCode = (short) (hashCode ^ (hashCode >>> 16));
        hashCode = hashCode < 0 ? hashCode * -1 : hashCode;
        String hashString = String.valueOf(hashCode);
        for (int i = 0, length = hashString.length(); i < length; i++) {
            int idx = hashString.charAt(i) - 48;// 48 - 57 are 0-9 numbers
            if (idx > 0 && idx < 3 && (i + 1) < length) {
                int c2 = hashString.charAt(i + 1) - 48;
                if (c2 < 6) {
                    idx = (idx * 10) + c2;
                    i++;
                }
            }
            builder.append(lowerCaseLetters[idx]);
        }
        return builder.toString();
    }

    /**
     * Convert the given name in camelCase to a lower_case string separating words by underscore.
     * @param defaultName the default name of the type/field to be converted.
     * @param givenName the specific name given, if present it will be returned.
     * @return the given name if present, or the underscore separated name
     */
    public static String getSqlName(String defaultName, String givenName) {
        if (givenName != null) {
            givenName = givenName.trim();
            if (!givenName.isEmpty()) {
                return givenName;
            }
        }
        return camelCaseToUnderscore(defaultName);
    }

    /**
     * Convert the given string in camelCase to a lower_case string separating words by underscore.
     * @param string the string
     * @return the underscore separated string
     */
    static String camelCaseToUnderscore(String string) {
        Objects.requireNonNull(string);
        StringBuilder builder = new StringBuilder(string);
        int length = builder.length();
        for (int i = 0; i < length; i++) {
            char ch = builder.charAt(i);
            if (Character.isUpperCase(ch)) { // ch >= 'A' && ch <= 'Z'
                if (i > 0) {
                    builder.insert(i++, '_');
                    length++;
                }
                builder.setCharAt(i, Character.toLowerCase(ch));// (char) (ch ^ 0x20)
            }
        }
        return builder.toString();
    }

    /**
     * Convert the given string with underscores to camelCase.
     * @param string the string
     * @return the converted string in camelCase
     */
    public static String underscoreToCamelCase(String string) {
        Objects.requireNonNull(string);
        final int idx = string.indexOf('_');
        if (idx == -1) {
            return string;
        }
        int length = string.length();
        StringBuilder builder = new StringBuilder(string);
        for (int i = idx; i < length; i++) {
            if (builder.charAt(i) == '_') {
                builder.deleteCharAt(i);
                if (i < --length) {
                    builder.setCharAt(i, Character.toUpperCase(builder.charAt(i)));
                }
            }
        }
        return builder.toString();
    }
}
