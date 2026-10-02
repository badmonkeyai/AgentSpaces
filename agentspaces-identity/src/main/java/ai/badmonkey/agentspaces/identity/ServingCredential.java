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
package ai.badmonkey.agentspaces.identity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Objects;

/**
 * The TLS credential this node serves on its channels: a private key and its
 * certificate chain. In the enterprise-CA mode (plan §3/WS4) the operator
 * loads a CA-issued credential whose leaf carries this peer's PeerID in the
 * subject CN — enrollment output, typically a PKCS#12 from the organization's
 * issuance pipeline. In the default self-signed mode this type is not needed:
 * {@link ChannelCertificate#generate} mints the identity-endorsed credential.
 *
 * @param key   the TLS private key
 * @param chain the certificate chain, leaf first
 */
public record ServingCredential(PrivateKey key, X509Certificate[] chain) {

    public ServingCredential {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(chain, "chain");
        if (chain.length == 0) {
            throw new IllegalArgumentException("the chain needs at least the leaf");
        }
        chain = chain.clone();
    }

    /**
     * Loads the first private-key entry from a keystore file (PKCS#12 or JKS).
     *
     * @param path     the keystore file
     * @param password the keystore/key password
     * @return the credential
     * @throws GeneralSecurityException when the store cannot be read or holds
     *                                  no usable private-key entry
     */
    public static ServingCredential fromKeyStore(Path path, char[] password)
            throws GeneralSecurityException {
        try (InputStream in = Files.newInputStream(path)) {
            KeyStore store = KeyStore.getInstance(
                    path.getFileName().toString().endsWith(".jks") ? "JKS" : "PKCS12");
            store.load(in, password);
            Enumeration<String> aliases = store.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (!store.isKeyEntry(alias)) {
                    continue;
                }
                Key key = store.getKey(alias, password);
                Certificate[] chain = store.getCertificateChain(alias);
                if (key instanceof PrivateKey privateKey && chain != null) {
                    X509Certificate[] x509 = new X509Certificate[chain.length];
                    for (int i = 0; i < chain.length; i++) {
                        x509[i] = (X509Certificate) chain[i];
                    }
                    return new ServingCredential(privateKey, x509);
                }
            }
            throw new GeneralSecurityException(
                    "no private-key entry with a chain in " + path);
        } catch (IOException e) {
            throw new GeneralSecurityException("cannot read keystore " + path, e);
        }
    }
}
