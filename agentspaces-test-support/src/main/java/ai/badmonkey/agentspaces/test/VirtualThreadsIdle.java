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
package ai.badmonkey.agentspaces.test;

import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * Waits until no virtual thread in this JVM runs or waits to run (srad/agentspaces-event-loop.md EL-Q-TST-03): the
 * settle a sim fleet on {@link VirtualTime} needs for the work that is not a task of its time, such as a content
 * swarm's frame handlers, which run one virtual thread per frame. {@link VirtualTime#advance} settles its own tasks;
 * this settles the rest, so a fleet ticks again only once the frames of the last tick are handled, with no fixed sleep.
 *
 * <p>It reads the JDK's virtual-thread scheduler ({@code jdk.management.VirtualThreadSchedulerMXBean}, JDK 24 and
 * later): idle is no virtual thread mounted (other than the caller's own) and none queued, read twice in a row. A
 * thread parked on a lock, a sleep or a wait is idle. On a JDK without the bean it only yields. Call it from the test's
 * thread between ticks; JUnit runs one test at a time in a JVM here, so the threads it sees are the test's.
 */
public final class VirtualThreadsIdle {

    private static final PlatformManagedObject BEAN;
    private static final Method MOUNTED;
    private static final Method QUEUED;

    static {
        PlatformManagedObject bean = null;
        Method mounted = null;
        Method queued = null;
        try {
            @SuppressWarnings("unchecked")
            Class<? extends PlatformManagedObject> type = (Class<? extends PlatformManagedObject>)
                    Class.forName("jdk.management.VirtualThreadSchedulerMXBean");
            bean = ManagementFactory.getPlatformMXBean(type);
            mounted = type.getMethod("getMountedVirtualThreadCount");
            queued = type.getMethod("getQueuedVirtualThreadCount");
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            bean = null; // an older JDK: yield only
        }
        BEAN = bean;
        MOUNTED = mounted;
        QUEUED = queued;
    }

    private VirtualThreadsIdle() {
    }

    /** Whether this JDK reports its virtual-thread scheduler, so {@link #await} really waits. */
    public static boolean available() {
        return BEAN != null;
    }

    /**
     * Waits until no virtual thread runs or waits to run, for at most {@code max} of real time.
     *
     * @param max the longest wait; a bound for a thread that spins, not a pause
     * @return true once idle; false at the bound or without the bean
     */
    public static boolean await(Duration max) {
        if (BEAN == null) {
            Thread.yield();
            return false;
        }
        int self = Thread.currentThread().isVirtual() ? 1 : 0;
        long deadline = System.nanoTime() + max.toNanos();
        int quiet = 0;
        while (true) {
            if (mounted() <= self && queued() == 0) {
                if (++quiet >= 2) {
                    return true;
                }
            } else {
                quiet = 0;
            }
            if (System.nanoTime() - deadline > 0) {
                return false;
            }
            LockSupport.parkNanos(20_000L);
        }
    }

    private static int mounted() {
        try {
            return (Integer) MOUNTED.invoke(BEAN);
        } catch (ReflectiveOperationException e) {
            return 0;
        }
    }

    private static long queued() {
        try {
            return (Long) QUEUED.invoke(BEAN);
        } catch (ReflectiveOperationException e) {
            return 0;
        }
    }
}
