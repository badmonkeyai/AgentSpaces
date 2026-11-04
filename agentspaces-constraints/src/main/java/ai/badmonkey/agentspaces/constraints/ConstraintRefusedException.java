/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.constraints;

/** Thrown to a bound method's caller when the gate refuses its result write (§9.6). */
public class ConstraintRefusedException extends RuntimeException {

    private final String unit;
    private final String key;

    public ConstraintRefusedException(String unit, String key, String reason) {
        super("constraint " + unit + " refuses the result for key " + key + ": " + reason);
        this.unit = unit;
        this.key = key;
    }

    public String unit() {
        return unit;
    }

    public String key() {
        return key;
    }
}
