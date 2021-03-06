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
package io.github.pustike.persist;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the column in an entity.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * The name of the column. Defaults to a provider-generated value.
     * @return The column name
     */
    String name() default "";

    /**
     * Whether the database column is optional.
     * @return whether this is optional
     */
    boolean optional() default true;

    /**
     * The column length. Applies only if a string-valued column is used.
     * @return length
     */
    int length() default 255;

    /**
     * The scale for a decimal (exact numeric) column. Applies only if a decimal column is used.
     * @return scale
     */
    int scale() default 0;

    /**
     * Whether to fetch when no @{@link FieldGroup} is specified in select queries. {@code true} by default.
     * @return whether to fetch this column
     */
    boolean fetch() default true;
}
