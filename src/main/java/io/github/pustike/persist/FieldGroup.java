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
 * Field Group to define list of fields to be included in the select or update queries.
 */
@Repeatable(FieldGroups.class)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldGroup {
    /**
     * Field Group name. Defaults to: {@code "default"}
     * @return the name of the fieldGroup
     */
    String name() default "default";

    /**
     * An array of fields to be included.
     * @return the field names array
     */
    String[] fields() default {};

    /**
     * Other fieldGroups to be included. Should be defined on the same entity before this fieldGroup.
     * @return an array of included field groups.
     */
    String[] includes() default {};

    /**
     * If {@code true}, this fieldGroup will be used to fetch fields when this entity is a foreign key instance.
     * Only one joinFetch type of fieldGroup can be present on an entity.
     * @return {@code true} if this fieldGroup to be used for join fetching
     */
    boolean joinFetch() default false;
}
