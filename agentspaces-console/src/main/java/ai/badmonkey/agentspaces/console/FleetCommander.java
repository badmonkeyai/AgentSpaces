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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The command-and-control executor: it carries out operator commands against
 * the fleet under the console peer's own fabric identity. Everything a command
 * does, dispatching a task, cancelling one the console dispatched, broadcasting
 * a worker directive, runs through here, so every command lands as a signed,
 * leased space entry and, when an audit space is configured, a durable record
 * in the C2 log. Commands also surface in the console's activity stream.
 *
 * <p>The commander does not authenticate; the HTTP layer does that (a bearer
 * token) before calling in, and passes the authenticated operator name. The
 * commander is the trust boundary between "the operator asked" and "the fabric
 * did it under the console peer's key on the operator's behalf".
 */
public final class FleetCommander {

    /** One durable C2 audit record, written to the audit space. */
    public record C2Record(String kind, String action, String target, String operator,
                           String detail, long issuedMillis) {
    }

    private final Function<String, Space> spaces;
    private final String controlSpaceName;
    private final String auditSpaceName;
    private final Duration directiveLease;
    private final Duration auditLease;
    private final ConsoleView view;
    private final InstantSource clock;
    private final Map<String, ConsoleCommand> commands;
    private final Map<String, EntryHandle> dispatched = new ConcurrentHashMap<>();

    private FleetCommander(Builder builder) {
        this.spaces = builder.spaces;
        this.controlSpaceName = builder.controlSpaceName;
        this.auditSpaceName = builder.auditSpaceName;
        this.directiveLease = builder.directiveLease;
        this.auditLease = builder.auditLease;
        this.view = builder.view;
        this.clock = builder.clock;
        this.commands = java.util.Collections.unmodifiableMap(
                new LinkedHashMap<>(builder.commands));
    }

    /** The registered command catalog, in registration order. */
    public List<ConsoleCommand> commands() {
        return new ArrayList<>(commands.values());
    }

    /** A registered command by id. */
    public Optional<ConsoleCommand> command(String id) {
        return Optional.ofNullable(commands.get(id));
    }

    /**
     * Broadcasts a control directive to the workers.
     *
     * @param action   the directive verb ({@link Directive#PAUSE} and friends)
     * @param target   the worker name, or {@link Directive#ALL}
     * @param operator the authenticated operator
     * @return the outcome
     */
    public ConsoleCommand.Outcome broadcast(String action, String target, String operator) {
        if (action == null || action.isBlank()) {
            return ConsoleCommand.Outcome.failed("action is required");
        }
        if (target == null || target.isBlank()) {
            return ConsoleCommand.Outcome.failed("target is required (a worker name or *)");
        }
        Space control = spaces.apply(controlSpaceName);
        if (control == null) {
            return ConsoleCommand.Outcome.failed(
                    "no control space '" + controlSpaceName + "' on this console");
        }
        Directive directive = new Directive(action.toUpperCase(java.util.Locale.ROOT),
                target, operator, nowMillis());
        control.write(directive, Lease.of(directiveLease));
        audit("directive", directive.action(), target, operator,
                "directive " + directive.action() + " -> " + target);
        return ConsoleCommand.Outcome.ok(
                directive.action() + " sent to " + target);
    }

    /**
     * Cancels an entry the console previously dispatched.
     *
     * @param entryId  the dispatched entry's id
     * @param operator the authenticated operator
     * @return the outcome
     */
    public ConsoleCommand.Outcome cancelDispatched(String entryId, String operator) {
        EntryHandle handle = dispatched.remove(entryId);
        if (handle == null) {
            return ConsoleCommand.Outcome.failed(
                    "no dispatched entry '" + entryId + "' to cancel "
                            + "(the console can only cancel entries it wrote)");
        }
        handle.cancel();
        audit("cancel", "CANCEL", entryId, operator, "cancelled dispatched entry " + entryId);
        return ConsoleCommand.Outcome.ok("cancelled " + entryId);
    }

    /**
     * Executes a registered command.
     *
     * @param id       the command id
     * @param args     the submitted field values
     * @param operator the authenticated operator
     * @return the outcome, or a failure when the command is unknown or throws
     */
    public ConsoleCommand.Outcome execute(String id, Map<String, String> args, String operator) {
        ConsoleCommand command = commands.get(id);
        if (command == null) {
            return ConsoleCommand.Outcome.failed("no such command: " + id);
        }
        for (ConsoleCommand.Field field : command.fields()) {
            if (field.required()
                    && (args.get(field.name()) == null || args.get(field.name()).isBlank())) {
                return ConsoleCommand.Outcome.failed("missing required field: " + field.name());
            }
        }
        try {
            ConsoleCommand.Outcome outcome = command.execute(args, new Ctx(operator));
            return outcome == null ? ConsoleCommand.Outcome.ok("done") : outcome;
        } catch (RuntimeException e) {
            return ConsoleCommand.Outcome.failed("command failed: " + e.getMessage());
        }
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    private void audit(String kind, String action, String target, String operator,
                       String detail) {
        if (auditSpaceName != null) {
            Space auditSpace = spaces.apply(auditSpaceName);
            if (auditSpace != null) {
                auditSpace.write(new C2Record(kind, action, target, operator, detail,
                        nowMillis()), Lease.of(auditLease));
            }
        }
        if (view != null) {
            view.recordCommand(kind, action, operator);
        }
    }

    /** The per-operator command context handed to a {@link ConsoleCommand}. */
    private final class Ctx implements CommandContext {
        private final String operator;

        private Ctx(String operator) {
            this.operator = operator;
        }

        @Override
        public String operator() {
            return operator;
        }

        @Override
        public String dispatch(String spaceName, Object entry, Duration lease) {
            Objects.requireNonNull(spaceName, "spaceName");
            Objects.requireNonNull(entry, "entry");
            Space space = spaces.apply(spaceName);
            if (space == null) {
                throw new IllegalArgumentException("no space '" + spaceName + "' on this console");
            }
            EntryHandle handle = space.write(entry, Lease.of(lease));
            String entryId = handle.entryId().value();
            dispatched.put(entryId, handle);
            FleetCommander.this.audit("dispatch", "DISPATCH", spaceName, operator,
                    "dispatched " + entry.getClass().getSimpleName() + " as " + entryId);
            return entryId;
        }

        @Override
        public boolean cancel(String entryId) {
            EntryHandle handle = dispatched.remove(entryId);
            if (handle == null) {
                return false;
            }
            handle.cancel();
            FleetCommander.this.audit("cancel", "CANCEL", entryId, operator, "cancelled " + entryId);
            return true;
        }

        @Override
        public void broadcast(String action, String target) {
            FleetCommander.this.broadcast(action, target, operator);
        }

        @Override
        public void audit(String action, String detail) {
            FleetCommander.this.audit("command", action, "", operator, detail);
        }
    }

    /**
     * Starts a builder.
     *
     * @param spaces resolves a space name to the console peer's space
     * @return the builder
     */
    public static Builder builder(Function<String, Space> spaces) {
        return new Builder(spaces);
    }

    /** Assembles a {@link FleetCommander}. */
    public static final class Builder {

        private final Function<String, Space> spaces;
        private final Map<String, ConsoleCommand> commands = new LinkedHashMap<>();
        private String controlSpaceName = "fleet-control";
        private String auditSpaceName;
        private Duration directiveLease = Duration.ofMinutes(30);
        private Duration auditLease = Duration.ofHours(24);
        private ConsoleView view;
        private InstantSource clock = InstantSource.system();

        private Builder(Function<String, Space> spaces) {
            this.spaces = Objects.requireNonNull(spaces, "spaces");
        }

        /**
         * Names the control space directives are written to (default
         * {@code fleet-control}); it must exist on the console peer.
         *
         * @param name the control space name
         * @return this builder
         */
        public Builder controlSpace(String name) {
            this.controlSpaceName = Objects.requireNonNull(name, "name");
            return this;
        }

        /**
         * Names the durable C2 audit space; when set, every command writes a
         * {@link C2Record} there. Omit for activity-stream-only auditing.
         *
         * @param name the audit space name, or {@code null}
         * @return this builder
         */
        public Builder auditSpace(String name) {
            this.auditSpaceName = name;
            return this;
        }

        /**
         * The lease directives are written under (default 30 minutes).
         *
         * @param lease the directive lease
         * @return this builder
         */
        public Builder directiveLease(Duration lease) {
            this.directiveLease = Objects.requireNonNull(lease, "lease");
            return this;
        }

        /**
         * Records commands in the console's activity stream.
         *
         * @param view the console view
         * @return this builder
         */
        public Builder view(ConsoleView view) {
            this.view = view;
            return this;
        }

        /**
         * Overrides the clock, for tests.
         *
         * @param clock the instant source stamped onto directives and audit records
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Registers a command.
         *
         * @param command the command
         * @return this builder
         */
        public Builder command(ConsoleCommand command) {
            Objects.requireNonNull(command, "command");
            if (commands.putIfAbsent(command.id(), command) != null) {
                throw new IllegalArgumentException("duplicate command id: " + command.id());
            }
            return this;
        }

        /**
         * Registers several commands.
         *
         * @param commands the commands
         * @return this builder
         */
        public Builder commands(List<? extends ConsoleCommand> commands) {
            commands.forEach(this::command);
            return this;
        }

        /** Builds the commander. */
        public FleetCommander build() {
            return new FleetCommander(this);
        }
    }
}
