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

import org.junit.jupiter.api.Test;

import static io.github.pustike.persist.utils.PersistUtils.camelCaseToUnderscore;
import static io.github.pustike.persist.utils.PersistUtils.getWrapperClass;
import static io.github.pustike.persist.utils.PersistUtils.underscoreToCamelCase;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersistUtilsTest {
    @Test
    public void getWrapperClassTests() {
        assertEquals(Integer.class, getWrapperClass(int.class));
        assertEquals(Double.class, getWrapperClass(double.class));
        assertEquals(Byte.class, getWrapperClass(byte.class));
        assertEquals(Boolean.class, getWrapperClass(boolean.class));
        assertEquals(Character.class, getWrapperClass(char.class));
        assertEquals(Void.class, getWrapperClass(void.class));
        assertEquals(Short.class, getWrapperClass(short.class));
        assertEquals(Float.class, getWrapperClass(float.class));
        assertEquals(Long.class, getWrapperClass(long.class));
        //
        assertEquals(String.class, getWrapperClass(String.class));
    }

    @Test
    public void camelCaseToUnderscoreTests() {
        assertEquals("camel_case", camelCaseToUnderscore("camelCase"));
        assertEquals("camel_case", camelCaseToUnderscore("CamelCase"));
        assertEquals("camel_case_t", camelCaseToUnderscore("CamelCaseT"));
        assertEquals("camel_case_t_e_s_t", camelCaseToUnderscore("CamelCaseTEST"));
        //
        assertEquals("camel_case", camelCaseToUnderscore("camel_case"));
    }

    @Test
    public void underscoreToCamelCaseTests() {
        assertEquals("camelCase", underscoreToCamelCase("camel_case"));
        assertEquals("camelCase", underscoreToCamelCase("camel_case_"));
        assertEquals("CamelCase", underscoreToCamelCase("_camel_case"));
        assertEquals("camel_case", underscoreToCamelCase("camel__case"));
        //
        assertEquals("camelCase", underscoreToCamelCase("camelCase"));
    }
}
