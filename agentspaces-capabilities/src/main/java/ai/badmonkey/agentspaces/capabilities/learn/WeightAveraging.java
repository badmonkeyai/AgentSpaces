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
package ai.badmonkey.agentspaces.capabilities.learn;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * The default {@link MergeableModel}: a {@code double[]} parameter vector whose
 * merge is the element-wise mean, so a fleet of models converges to the fleet
 * mean and every exchange conserves the fleet sum. Encoding is big-endian IEEE
 * 754 doubles, 8 bytes per parameter, which makes the content address stable
 * across platforms. Vectors of different length are incompatible.
 */
public final class WeightAveraging implements MergeableModel<double[]> {

    /** The shared instance; the class is stateless. */
    public static final WeightAveraging INSTANCE = new WeightAveraging();

    @Override
    public double[] merge(double[] a, double[] b) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "parameter vectors differ in length: " + a.length + " vs " + b.length);
        }
        double[] merged = new double[a.length];
        for (int i = 0; i < merged.length; i++) {
            merged[i] = (a[i] + b[i]) / 2.0;
        }
        return merged;
    }

    @Override
    public byte[] encode(double[] model) {
        Objects.requireNonNull(model, "model");
        ByteBuffer buffer = ByteBuffer.allocate(model.length * Double.BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        for (double d : model) {
            buffer.putDouble(d);
        }
        return buffer.array();
    }

    @Override
    public double[] decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length % Double.BYTES != 0) {
            throw new IllegalArgumentException("not a vector of doubles: " + bytes.length + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        double[] model = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < model.length; i++) {
            model[i] = buffer.getDouble();
        }
        return model;
    }

    @Override
    public String name() {
        return "weight-averaging";
    }
}
