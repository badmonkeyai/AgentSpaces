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
package ai.badmonkey.agentspaces.common.crypto;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The seal budget of a group content key (SPEC §11a.3 / ASF-045): random 96-bit
 * GCM nonces make collision risk material near 2^32 encryptions, so the key
 * counts its seals, warns at 2^31, and refuses past 2^32. Driving the counter
 * through 2^32 real encryptions is infeasible in a unit test, so these tests
 * position the private counter by reflection and exercise the boundaries.
 */
class GroupKeySealBudgetTest {

    private static final long WARN_THRESHOLD = 1L << 31;
    private static final long CEILING = 1L << 32;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Positions the key's seal counter as if {@code count} seals had already happened. */
    private static void setSeals(GroupKey key, long count) throws Exception {
        Field field = GroupKey.class.getDeclaredField("seals");
        field.setAccessible(true);
        ((AtomicLong) field.get(key)).set(count);
    }

    private static long seals(GroupKey key) throws Exception {
        Field field = GroupKey.class.getDeclaredField("seals");
        field.setAccessible(true);
        return ((AtomicLong) field.get(key)).get();
    }

    /** SPEC §11a.3 / ASF-045: the 2^32-nd seal is the last one honoured; the next refuses with a rekey demand. */
    @Test
    void refusesToSealPastTheNonceBudget() throws Exception {
        GroupKey key = GroupKey.generate();
        setSeals(key, CEILING - 1);

        byte[] last = key.encrypt(bytes("the last permitted seal"), bytes("entry-last"));
        assertThat(key.decrypt(last, bytes("entry-last")))
                .as("seal number 2^32 still round-trips")
                .hasValueSatisfying(plain ->
                        assertThat(plain).isEqualTo(bytes("the last permitted seal")));
        assertThat(seals(key)).isEqualTo(CEILING);

        assertThatThrownBy(() -> key.encrypt(bytes("one too many"), bytes("entry-over")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2^32")
                .hasMessageContaining("rotate");
        // The refusal is permanent for this key: every further seal fails too.
        assertThatThrownBy(() -> key.encrypt(bytes("still refused"), bytes("entry-over-2")))
                .isInstanceOf(IllegalStateException.class);

        // Reading never stops: ciphertexts sealed under the exhausted key remain
        // decryptable, so an exhausted key is a write-side rekey demand only.
        assertThat(key.decrypt(last, bytes("entry-last"))).isPresent();
    }

    /** SPEC §11a.3: the 2^31 warning threshold is advisory only — sealing continues normally through and past it. */
    @Test
    void theWarningThresholdDoesNotInterruptSealing() throws Exception {
        GroupKey key = GroupKey.generate();
        setSeals(key, WARN_THRESHOLD - 1);

        byte[] atThreshold = key.encrypt(bytes("2^31st"), bytes("aad"));
        byte[] pastThreshold = key.encrypt(bytes("2^31st + 1"), bytes("aad"));

        assertThat(seals(key)).isEqualTo(WARN_THRESHOLD + 1);
        assertThat(key.decrypt(atThreshold, bytes("aad")))
                .hasValueSatisfying(p -> assertThat(p).isEqualTo(bytes("2^31st")));
        assertThat(key.decrypt(pastThreshold, bytes("aad")))
                .hasValueSatisfying(p -> assertThat(p).isEqualTo(bytes("2^31st + 1")));
    }

    /** Characterization (SPEC §11a.3 open point): the seal budget is per in-memory key instance — restoring the same key material through fromBytes starts a fresh counter, so persistence does not carry the nonce history. */
    @Test
    void restoringTheKeyMaterialResetsTheSealCounter() throws Exception {
        GroupKey exhausted = GroupKey.generate();
        setSeals(exhausted, CEILING);
        assertThatThrownBy(() -> exhausted.encrypt(bytes("x"), bytes("aad")))
                .isInstanceOf(IllegalStateException.class);

        GroupKey restored = GroupKey.fromBytes(exhausted.rawBytes());
        assertThat(seals(restored)).as("a restored key knows nothing of past seals").isZero();
        byte[] sealed = restored.encrypt(bytes("after restore"), bytes("aad"));
        assertThat(seals(restored)).isEqualTo(1);
        // Same key material: the exhausted instance can still read what the
        // restored one sealed, which is exactly why the reset is a deployment
        // responsibility (rotate, do not merely restart) and is pinned here.
        assertThat(exhausted.decrypt(sealed, bytes("aad")))
                .hasValueSatisfying(p -> assertThat(p).isEqualTo(bytes("after restore")));
    }

    /** SPEC §11a.3: every seal, and only a seal, advances the budget; decryption is free. */
    @Test
    void everySealCountsAndDecryptionDoesNot() throws Exception {
        GroupKey key = GroupKey.generate();
        assertThat(seals(key)).isZero();

        byte[] a = key.encrypt(bytes("a"), bytes("1"));
        byte[] b = key.encrypt(bytes("b"), bytes("2"));
        assertThat(seals(key)).isEqualTo(2);

        key.decrypt(a, bytes("1"));
        key.decrypt(b, bytes("2"));
        key.decrypt(b, bytes("wrong-aad"));
        assertThat(seals(key)).as("reads do not spend the nonce budget").isEqualTo(2);
    }
}
