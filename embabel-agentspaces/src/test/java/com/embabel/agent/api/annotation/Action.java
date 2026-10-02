/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.embabel.agent.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Test-scope stand-in for Embabel 1.5's {@code @Action}, carrying the attributes
 * of the real annotation whose types are not Embabel's own.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Action {
    String description() default "";
    String[] pre() default {};
    String[] post() default {};
    boolean canRerun() default false;
    boolean readOnly() default false;
    boolean clearBlackboard() default false;
    String outputBinding() default "";
    double cost() default 0.0;
    double value() default 0.0;
    String costMethod() default "";
    String valueMethod() default "";
    long delayMs() default 0L;
}
