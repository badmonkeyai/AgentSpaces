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
package ai.badmonkey.agentspaces.transport.http;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Address parsing (FR-2) and the origin policy (SR-5, JS-H-03). */
class HttpAddressTest {

    @Test
    void parsesHostPortAndPathWithTheDefault() {
        assertThat(HttpAddress.parse("fleet.example.com:443/aspace", "/x"))
                .isEqualTo(new HttpAddress("fleet.example.com", 443, "/aspace"));
        assertThat(HttpAddress.parse("10.0.0.5:7503", "/aspace").path()).isEqualTo("/aspace");
        assertThat(HttpAddress.parse("[::1]:7503/aspace", "/x").host()).isEqualTo("::1");
        assertThat(HttpAddress.parse("[::1]:7503", "/aspace").authority()).isEqualTo("[::1]:7503");
        assertThat(HttpAddress.parse("h:1/a", "/x").url("wss")).isEqualTo("wss://h:1/a");
        assertThatThrownBy(() -> HttpAddress.parse("no-port", "/x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HttpAddress.parse("h:99999", "/x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HttpAddress.parse("h:1/a?b", "/x")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theOriginPolicyAdmitsNonBrowsersAndOnlyListedOrigins() {
        OriginPolicy none = OriginPolicy.none();
        assertThat(none.allows(null)).isTrue();
        assertThat(none.allows("https://app.example.com")).isFalse();
        assertThat(none.admitsBrowsers()).isFalse();
        OriginPolicy some = OriginPolicy.of(List.of("https://App.Example.com", "http://localhost:8080"));
        assertThat(some.allows("https://app.example.com")).isTrue();
        assertThat(some.allows("http://localhost:8080")).isTrue();
        assertThat(some.allows("http://localhost:8081")).isFalse();
        assertThat(some.allows("https://evil.example.com")).isFalse();
        assertThat(some.allows("not an origin")).isFalse();
        assertThat(OriginPolicy.of(List.of("*")).allows("https://anything.example")).isTrue();
        assertThatThrownBy(() -> OriginPolicy.of(List.of("https://app.example.com/path")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void optionsRefuseNonsense() {
        HttpOptions defaults = HttpOptions.defaults();
        assertThat(defaults.path()).isEqualTo("/aspace");
        assertThat(defaults.exchangePath()).isEqualTo("/aspace/h");
        assertThatThrownBy(() -> defaults.withPath("relative")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withLongPoll(java.time.Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withOpensPerSourcePerSecond(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
