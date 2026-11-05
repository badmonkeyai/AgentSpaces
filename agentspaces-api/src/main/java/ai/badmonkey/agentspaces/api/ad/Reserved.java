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


package ai.badmonkey.agentspaces.api.ad;

import java.util.List;

/**
 * Names the specification reserves without a core implementation behind them (SPEC §6.5a, §7.6; v0.1.27,
 * ISSUE-CommerceCapability): a capability type a peer advertises to say what it does in a trade, the roles it may
 * name, and the space a trading group keeps its mandates in. The records that travel there live in the open
 * {@code agentspaces-commerce} module; the behaviour is a platform's.
 */
public final class Reserved {

    /** The commerce capability: a peer that acts as a merchant, a shopper, or a payment party. */
    public static final String CAPABILITY_COMMERCE = "aspace:cap/commerce";
    /** The {@code role} parameter of the commerce capability. */
    public static final String ROLE = "role";
    public static final String ROLE_MERCHANT = "merchant";
    public static final String ROLE_SHOPPER = "shopper";
    public static final String ROLE_PAYMENT = "payment";
    public static final List<String> COMMERCE_ROLES = List.of(ROLE_MERCHANT, ROLE_SHOPPER, ROLE_PAYMENT);
    /** The optional parameters: the merchant's business profile URL, the UCP capability ids served, the signing key as a JWK. */
    public static final String PROFILE = "profile";
    public static final String UCP = "ucp";
    public static final String JWK = "jwk";
    /** The space a trading group keeps its mandates and orders in: LEASE_RACE, authorizer admission, one year. */
    public static final String SPACE_COMMERCE = "commerce";

    private Reserved() {
    }
}
