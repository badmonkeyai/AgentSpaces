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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * The orders of the three DataScript indexes (PDT 2 phase 2, P2-D-11; srad/agentspaces-pdt2.md
 * §3.1.1) over decoded datoms, matching DataScript 1.8's {@code cmp-datoms-eavt}, {@code -aevt},
 * and {@code -avet}, so a Java walk of an index DataScript wrote finds what DataScript finds.
 *
 * <ul>
 *   <li>An attribute compares as a Clojure keyword: no namespace first, then the namespace, then
 *       the name, split at the first {@code /} of the print form.</li>
 *   <li>A value compares as DataScript's {@code value-compare} does over the Clojure value a datom
 *       value decodes to: numbers numerically across integer, float, and big integer; two values of
 *       one comparable class by that class; any other pair by the Java class name of the Clojure
 *       value ({@code clojure.lang.Keyword} before {@code java.lang.Boolean} before
 *       {@code java.lang.String}, and so on). Text is a keyword when it starts with {@code :} and has
 *       a name, a string when it starts with a backslash (one backslash dropped), and a string
 *       otherwise, the rule of the Clojure face. Byte strings compare by content, where DataScript
 *       falls back to identity hashes; two byte values of one attribute and entity therefore keep
 *       DataScript's order only when they are equal.</li>
 *   <li>The transaction compares by magnitude, since its sign marks a retraction.</li>
 * </ul>
 */
public final class DatomOrder {

    /** Entity, attribute, value, transaction. */
    public static final Comparator<Datom> EAVT = (x, y) -> {
        int c = Long.compare(x.e(), y.e());
        if (c == 0) {
            c = compareAttributes(x.a(), y.a());
        }
        if (c == 0) {
            c = compareValues(x.v(), y.v());
        }
        return c != 0 ? c : Long.compare(Math.abs(x.tx()), Math.abs(y.tx()));
    };

    /** Attribute, entity, value, transaction. */
    public static final Comparator<Datom> AEVT = (x, y) -> {
        int c = compareAttributes(x.a(), y.a());
        if (c == 0) {
            c = Long.compare(x.e(), y.e());
        }
        if (c == 0) {
            c = compareValues(x.v(), y.v());
        }
        return c != 0 ? c : Long.compare(Math.abs(x.tx()), Math.abs(y.tx()));
    };

    /** Attribute, value, entity, transaction. */
    public static final Comparator<Datom> AVET = (x, y) -> {
        int c = compareAttributes(x.a(), y.a());
        if (c == 0) {
            c = compareValues(x.v(), y.v());
        }
        if (c == 0) {
            c = Long.compare(x.e(), y.e());
        }
        return c != 0 ? c : Long.compare(Math.abs(x.tx()), Math.abs(y.tx()));
    };

    private DatomOrder() {
    }

    /**
     * The order of an index by its {@code x} name.
     *
     * @param index {@code eavt}, {@code aevt}, or {@code avet}
     * @return the comparator
     */
    public static Comparator<Datom> of(String index) {
        return switch (index) {
            case DnNode.EAVT -> EAVT;
            case DnNode.AEVT -> AEVT;
            case DnNode.AVET -> AVET;
            default -> throw new IllegalArgumentException("no index order for '" + index + "'; the indexes are eavt, aevt and avet");
        };
    }

    /** Two attribute print forms as Clojure compares the keywords. */
    public static int compareAttributes(String a, String b) {
        if (a.equals(b)) {
            return 0;
        }
        String[] x = split(a);
        String[] y = split(b);
        if (x[0] == null || y[0] == null) {
            if (x[0] != null) {
                return 1;
            }
            if (y[0] != null) {
                return -1;
            }
        } else {
            int c = x[0].compareTo(y[0]);
            if (c != 0) {
                return c;
            }
        }
        return x[1].compareTo(y[1]);
    }

    /** A keyword's print form as {namespace or null, name}, split at the first slash as {@code Symbol.intern} does. */
    private static String[] split(String printForm) {
        String body = printForm.startsWith(":") ? printForm.substring(1) : printForm;
        int slash = body.indexOf('/');
        if (slash <= 0 || body.equals("/")) {
            return new String[]{null, body};
        }
        return new String[]{body.substring(0, slash), body.substring(slash + 1)};
    }

    /** Two decoded datom values as DataScript's {@code value-compare} orders the Clojure values. */
    public static int compareValues(Object x, Object y) {
        Kind kx = kindOf(x);
        Kind ky = kindOf(y);
        if (kx == Kind.NUMBER && ky == Kind.NUMBER) {
            return compareNumbers(x, y);
        }
        if (kx != ky) {
            return className(x, kx).compareTo(className(y, ky));
        }
        return switch (kx) {
            case KEYWORD -> compareAttributes((String) x, (String) y);
            case STRING -> string((String) x).compareTo(string((String) y));
            case BOOLEAN -> Boolean.compare((Boolean) x, (Boolean) y);
            case BYTES -> Arrays.compare((byte[]) x, (byte[]) y);
            case INSTANT -> Long.compare((Long) ((Datom.Tagged) x).inner(), (Long) ((Datom.Tagged) y).inner());
            case UUID -> uuid((Datom.Tagged) x).compareTo(uuid((Datom.Tagged) y));
            default -> throw new IllegalArgumentException("a datom value that orders by no rule: " + x);
        };
    }

    /** The Clojure class a decoded value stands for, named as DataScript's class order names it. */
    private enum Kind {
        BYTES("[B"), KEYWORD("clojure.lang.Keyword"), BOOLEAN("java.lang.Boolean"), NUMBER(null),
        STRING("java.lang.String"), INSTANT("java.util.Date"), UUID("java.util.UUID");

        final String className;

        Kind(String className) {
            this.className = className;
        }
    }

    /** The class name of the Clojure value: a number's by its type, any other by its kind. */
    private static String className(Object v, Kind kind) {
        if (kind != Kind.NUMBER) {
            return kind.className;
        }
        return v instanceof Long ? "java.lang.Long" : v instanceof Double ? "java.lang.Double" : "clojure.lang.BigInt";
    }

    private static Kind kindOf(Object v) {
        if (v instanceof Long || v instanceof Double) {
            return Kind.NUMBER;
        }
        if (v instanceof String s) {
            return s.length() > 1 && s.charAt(0) == ':' ? Kind.KEYWORD : Kind.STRING;
        }
        if (v instanceof Boolean) {
            return Kind.BOOLEAN;
        }
        if (v instanceof byte[]) {
            return Kind.BYTES;
        }
        if (v instanceof Datom.Tagged t) {
            return switch (t.tag()) {
                case Datom.Tagged.BIGINT -> Kind.NUMBER;
                case Datom.Tagged.INSTANT -> Kind.INSTANT;
                default -> Kind.UUID;
            };
        }
        throw new IllegalArgumentException("a spilled datom value orders only once it is read back: " + v);
    }

    /** The string a text value stands for: one leading backslash dropped. */
    private static String string(String text) {
        return !text.isEmpty() && text.charAt(0) == '\\' ? text.substring(1) : text;
    }

    private static int compareNumbers(Object x, Object y) {
        if (x instanceof Double || y instanceof Double) {
            // Clojure's Numbers.compare: -0.0 equals 0.0, where Double.compare orders them
            double a = doubleOf(x);
            double b = doubleOf(y);
            return a < b ? -1 : a > b ? 1 : 0;
        }
        return bigOf(x).compareTo(bigOf(y));
    }

    private static double doubleOf(Object v) {
        return v instanceof Datom.Tagged t ? new BigInteger((byte[]) t.inner()).doubleValue() : ((Number) v).doubleValue();
    }

    private static BigInteger bigOf(Object v) {
        return v instanceof Datom.Tagged t ? new BigInteger((byte[]) t.inner()) : BigInteger.valueOf((Long) v);
    }

    private static java.util.UUID uuid(Datom.Tagged t) {
        ByteBuffer b = ByteBuffer.wrap((byte[]) t.inner());
        return new java.util.UUID(b.getLong(), b.getLong());
    }
}
