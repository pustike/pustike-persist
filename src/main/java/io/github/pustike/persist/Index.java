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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Index annotation is used in schema generation. The @Index can be defined on a Entity class, or on an attribute.
 * The column is defaulted when defined on a attribute.
 */
@Repeatable(Indexes.class)
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    /**
     * The name of the index. Defaults to a provider-generated value.
     * @return The index name
     */
    String name() default "";

    /**
     * The names of the columns to be included in the index. Not required when annotated on a field.
     * @return The names of the columns making up the index
     */
    String[] columns() default {};
}
