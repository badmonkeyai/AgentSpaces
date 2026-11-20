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

package org.springframework.boot.availability;

import org.springframework.context.ApplicationEventPublisher;

/**
 * Build-time stub of Spring Boot's AvailabilityChangeEvent: the constructor, {@code getState} and the static
 * {@code publish} the starter calls, with the real signatures; the real class replaces it at runtime.
 *
 * @param <S> the availability state type
 */
public class AvailabilityChangeEvent<S extends AvailabilityState> extends java.util.EventObject {

    private final S state;

    public AvailabilityChangeEvent(Object source, S state) {
        super(source);
        this.state = state;
    }

    public S getState() {
        return state;
    }

    public static <S extends AvailabilityState> void publish(ApplicationEventPublisher publisher, Object source,
                                                            S state) {
        publisher.publishEvent(new AvailabilityChangeEvent<>(source, state));
    }
}
