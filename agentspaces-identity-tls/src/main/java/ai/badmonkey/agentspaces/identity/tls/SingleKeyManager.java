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

package ai.badmonkey.agentspaces.identity.tls;

import ai.badmonkey.agentspaces.identity.ServingCredential;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Serves exactly one credential and its key, for any request: the channel
 * certificate (identity-endorsed or CA-issued) is the only thing an attesting
 * binding ever presents, so there is nothing to choose between.
 */
public final class SingleKeyManager extends X509ExtendedKeyManager {

    private static final String ALIAS = "channel";

    private final ServingCredential credential;

    /**
     * Creates the key manager.
     *
     * @param credential the credential to present
     */
    public SingleKeyManager(ServingCredential credential) {
        this.credential = Objects.requireNonNull(credential, "credential");
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return new String[]{ALIAS};
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        return ALIAS;
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        return ALIAS;
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return new String[]{ALIAS};
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        return ALIAS;
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        return ALIAS;
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        return credential.chain();
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        return credential.key();
    }
}
