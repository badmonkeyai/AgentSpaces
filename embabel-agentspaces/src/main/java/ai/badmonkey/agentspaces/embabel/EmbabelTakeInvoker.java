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
package ai.badmonkey.agentspaces.embabel;

import ai.badmonkey.agentspaces.agent.TakeContext;
import ai.badmonkey.agentspaces.agent.spi.TakeInvoker;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a worker {@code @Action} as an Embabel process (reviews/embabel-agentspaces-review-2026-11-12.md
 * §7.8): a {@code @SpaceTake} method that takes {@code OperationContext} (or anything else Embabel resolves)
 * beside its entry cannot be called directly, so each taken entry runs as one process on <em>the bean's own
 * agent</em>, aimed at one goal, an instance of the method's return type. The scope is the own agent alone,
 * never the platform or the fleet agent, so a served task cannot call back out into the fleet through the
 * planner. The take lease is renewed while the process runs, and a process past {@code maxRun} is killed so
 * the lease lapses. A process that finds no plan completes the take with no result, so the caller times out
 * once instead of the fleet retrying forever; a failed process lets the lease lapse, as a thrown action does.
 *
 * <p>Under {@code take-mode: auto} the invoker claims an {@code @Action} worker method only when it declares
 * a parameter after the entry; under {@code platform} it claims every {@code @Action} worker method. Reaches
 * Embabel reflectively, so the module still builds without it; the platform is attached when Spring creates
 * the {@code AgentPlatform} bean.
 */
public final class EmbabelTakeInvoker implements TakeInvoker {

    private static final System.Logger LOG = System.getLogger(EmbabelTakeInvoker.class.getName());
    private static final String ACTION_ANNOTATION = "com.embabel.agent.api.annotation.Action";
    private static final String GOAL = "com.embabel.agent.core.Goal";
    private static final String AGENT = "com.embabel.agent.core.Agent";
    private static final String PROCESS_OPTIONS = "com.embabel.agent.core.ProcessOptions";

    /** How much the invoker claims. */
    public enum Mode { AUTO, PLATFORM }

    private static volatile Object attachedPlatform;

    private final Mode mode;
    private final Duration maxRun;

    public EmbabelTakeInvoker(Mode mode, Duration maxRun) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.maxRun = Objects.requireNonNull(maxRun, "maxRun");
    }

    /** Attaches the platform served tasks run on; called when Spring creates the {@code AgentPlatform} bean. */
    public static void attach(Object platform) {
        attachedPlatform = Objects.requireNonNull(platform, "platform");
    }

    /** Detaches the platform, for tests. */
    public static void detach() {
        attachedPlatform = null;
    }

    @Override
    public boolean claims(Object bean, Method method) {
        if (!EmbabelIntrospector.isAction(method)) {
            return false;
        }
        return mode == Mode.PLATFORM || method.getParameterCount() > 1;
    }

    @Override
    public void validate(Object bean, Method method) {
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 1; i < parameters.length; i++) {
            if (!parameters[i].getName().startsWith("com.embabel.")) {
                throw new IllegalArgumentException("a platform-served @SpaceTake method takes its entry, then only"
                        + " parameters Embabel resolves; parameter " + i + " is " + parameters[i].getName()
                        + ": " + method);
            }
        }
        if (method.getReturnType() == void.class) {
            throw new IllegalArgumentException("a platform-served @SpaceTake method returns the entry its process"
                    + " produces: " + method);
        }
    }

    @Override
    public Object invoke(Object bean, Method method, Object entry, TakeContext context) throws Exception {
        Object platform = attachedPlatform;
        if (platform == null) {
            throw new IllegalStateException("no AgentPlatform is attached yet; the take lapses for a later worker");
        }
        String agentName = EmbabelIntrospector.introspect(bean.getClass())
                .map(EmbabelIntrospector.EmbabelAgent::name)
                .orElseThrow(() -> new IllegalStateException(bean.getClass().getName() + " is not an @Agent"));
        Object own = ownAgent(platform, agentName);
        Object scoped = scopedToResult(own, method.getReturnType());
        Object process = createProcess(platform, scoped, entry);

        CompletableFuture<Object> run = CompletableFuture.supplyAsync(() -> {
            // The take context rides the thread that runs the process, so an action Embabel executes on this
            // thread finds TakeContext.current() (renewal, the agent identity of nested writes).
            TakeContext.bind(context);
            try {
                return process.getClass().getMethod("run").invoke(process);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("the process failed to run", e.getCause() != null ? e.getCause() : e);
            } finally {
                TakeContext.unbind();
            }
        }, runnable -> Thread.ofVirtual().name("embabel-served-" + agentName + "." + method.getName()).start(runnable));

        long renewEvery = Math.max(250, context.lease().toMillis() / 3);
        long deadline = System.nanoTime() + maxRun.toNanos();
        while (true) {
            try {
                run.get(renewEvery, TimeUnit.MILLISECONDS);
                break;
            } catch (TimeoutException e) {
                if (System.nanoTime() >= deadline) {
                    kill(process);
                    throw new IllegalStateException(agentName + "." + method.getName() + " ran past " + maxRun
                            + "; killed, the take lapses");
                }
                try {
                    context.renew();
                } catch (RuntimeException renewal) {
                    LOG.log(System.Logger.Level.DEBUG, "lease renewal failed; the run continues", renewal);
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw cause instanceof Exception failure ? failure : new IllegalStateException(cause);
            }
        }
        String status = statusOf(process);
        if ("FAILED".equals(status)) {
            throw new IllegalStateException(agentName + "." + method.getName() + ": the process failed");
        }
        if (!"COMPLETED".equals(status)) {
            LOG.log(System.Logger.Level.WARNING, agentName + "." + method.getName() + ": the process ended " + status
                    + "; the take completes with no result");
            return null;
        }
        return resultOf(process, method.getReturnType());
    }

    private static Object ownAgent(Object platform, String agentName) throws ReflectiveOperationException {
        Object listed = platform.getClass().getMethod("agents").invoke(platform);
        if (listed instanceof Iterable<?> agents) {
            for (Object agent : agents) {
                if (agentName.equals(agent.getClass().getMethod("getName").invoke(agent))) {
                    return agent;
                }
            }
        }
        throw new IllegalStateException("the platform holds no agent named '" + agentName
                + "'; is the bean deployed to Embabel?");
    }

    /**
     * The own agent with one goal, an instance of the result type, so the planner runs toward exactly what the
     * take expects; the agent itself, with its own goals, when Embabel's constructors differ from 1.5's.
     */
    private static Object scopedToResult(Object own, Class<?> resultType) {
        try {
            Class<?> goalType = Class.forName(GOAL, false, own.getClass().getClassLoader());
            Object goal = goalType.getMethod("createInstance", String.class, Class.class, String.class, Set.class)
                    .invoke(null, "Produce a " + resultType.getSimpleName() + " for the taken entry", resultType,
                            "produce" + resultType.getSimpleName(), Set.of());
            Class<?> agentType = Class.forName(AGENT, false, own.getClass().getClassLoader());
            Class<?> stuckHandler = Class.forName("com.embabel.agent.api.common.StuckHandler", false,
                    own.getClass().getClassLoader());
            var constructor = agentType.getConstructor(String.class, String.class, String.class, String.class,
                    Set.class, List.class, Set.class, stuckHandler);
            return constructor.newInstance(
                    own.getClass().getMethod("getName").invoke(own),
                    own.getClass().getMethod("getProvider").invoke(own),
                    own.getClass().getMethod("getVersion").invoke(own),
                    own.getClass().getMethod("getDescription").invoke(own),
                    Set.of(goal),
                    own.getClass().getMethod("getActions").invoke(own),
                    own.getClass().getMethod("getConditions").invoke(own),
                    own.getClass().getMethod("getStuckHandler").invoke(own));
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "cannot scope the agent to one goal; running it as is", e);
            return own;
        }
    }

    private static Object createProcess(Object platform, Object agent, Object entry) throws ReflectiveOperationException {
        Class<?> optionsType = Class.forName(PROCESS_OPTIONS, false, platform.getClass().getClassLoader());
        Object options = optionsType.getField("DEFAULT").get(null);
        for (Method method : platform.getClass().getMethods()) {
            if (method.getName().equals("createAgentProcess") && method.getParameterCount() == 3
                    && method.getParameterTypes()[2] == Map.class) {
                return method.invoke(platform, agent, options, Map.of("it", entry));
            }
        }
        throw new IllegalStateException("the platform has no createAgentProcess(Agent, ProcessOptions, Map)");
    }

    private static String statusOf(Object process) {
        try {
            Object status = process.getClass().getMethod("getStatus").invoke(process);
            return status instanceof Enum<?> e ? e.name() : String.valueOf(status);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return "UNKNOWN";
        }
    }

    private static Object resultOf(Object process, Class<?> resultType) {
        try {
            return process.getClass().getMethod("resultOfType", Class.class).invoke(process, resultType);
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "the process completed without a " + resultType.getSimpleName()
                    + "; the take completes with no result");
            return null;
        }
    }

    private static void kill(Object process) {
        try {
            process.getClass().getMethod("kill").invoke(process);
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "kill failed", e);
        }
    }

    /** The claimed methods of a class, for diagnostics. */
    static List<Method> claimedMethods(EmbabelTakeInvoker invoker, Object bean) {
        List<Method> claimed = new ArrayList<>();
        for (Method method : bean.getClass().getMethods()) {
            if (invoker.claims(bean, method)) {
                claimed.add(method);
            }
        }
        return claimed;
    }
}
