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

package org.springframework.beans.factory;

/**
 * Build-time stub of Spring's ObjectProvider, declaring only the methods the
 * auto-configuration calls; the real interface (which inherits
 * {@code getObject()} from {@code ObjectFactory}) replaces it at runtime.
 *
 * @param <T> the object type
 */
public interface ObjectProvider<T> {

    T getObject();

    T getIfAvailable();
}
