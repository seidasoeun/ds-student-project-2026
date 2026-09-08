package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A replica of the distributed positions' database.
 *
 * <p>
 * Each replica keeps a full copy of the array of positions {@code P}. Read
 * requests are answered locally; write requests are forwarded to the current
 * coordinator, which runs a two-phase, quorum-based total order broadcast
 * (UPDATE / ACK / WRITEOK). When the coordinator is detected as crashed, the
 * surviving replicas run a ring-based election and the most up-to-date replica
 * takes over, starting a new epoch.
 * </p>
 */
public class Replica extends AbstractReplica {

    // =================================================================================
    // Update identifiers and update records
    // =================================================================================

    /**
         * Identifier of an update: the pair &lt;e, i&gt; where {@code e} is the epoch
         * and {@code i} the sequence number inside the epoch. The natural order is the
         * lexicographic one: &lt;e,i&gt; precedes &lt;e',i'&gt; iff e &lt; e' or
         * (e == e' and i &lt; i').
         */
        public record UpdateID(int epoch, int sequence) implements Serializable, Comparable<UpdateID> {
            /**
             * Sentinel used by replicas that have not seen any update yet.
             */
            public static final UpdateID NONE = new UpdateID(-1, -1);

        @Override
            public int compareTo(UpdateID other) {
                if (this.epoch != other.epoch) {
                    return Integer.compare(this.epoch, other.epoch);
                }
                return Integer.compare(this.sequence, other.sequence);
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof UpdateID(int epoch1, int sequence1))) {
                    return false;
                }
                return epoch1 == this.epoch && sequence1 == this.sequence;
            }

        @Override
            public String toString() {
                return epoch + ":" + sequence;
            }
        }

    // =================================================================================
    // Protocol messages
    // =================================================================================

    /**
     * Client -&gt; replica: read the value stored at {@code index}.
     */
        public record ClientRead(int index, long requestId) implements Serializable {
    }

    /**
     * Replica -&gt; client: answer to a {@link ClientRead}.
     */
        public record ClientReadReply(int index, int value, int replicaId, long requestId) implements Serializable {
    }

    /**
     * Client -&gt; replica: set {@code P[index] = value}.
     */
        public record ClientWrite(int index, int value, long requestId) implements Serializable {
    }

    /**
     * Replica -&gt; client: to write has been applied by the system.
     */
        public record ClientWriteReply(int index, int value, int replicaId, long requestId) implements Serializable {
    }

    /**
     * Replica -&gt; coordinator: please run the update protocol for this write.
     */
        public record Forward(int originId, long writeId, ActorRef client, int index, int value) implements Serializable {
    }

    /**
     * Coordinator -&gt; all replicas: phase 1 of the total order broadcast.
     *
     * @param originId Replica that received the client request (used to answer the client).
     * @param writeId  Request identifier local to the origin replica.
     */
        public record Update(UpdateID uid, int index, int value, int originId, long writeId,
                             ActorRef client) implements Serializable {

        @Override
            public String toString() {
                return "Update(" + uid + ", " + index + ", " + value + ")";
            }
        }

    /**
     * Replica -&gt; coordinator: acknowledgement of an {@link Update}.
     */
        public record Ack(UpdateID uid, int fromId) implements Serializable {
    }

    /**
     * Coordinator -&gt; all replicas: phase 2, the update can be applied.
     */
        public record WriteOk(UpdateID uid) implements Serializable {
    }

    /**
     * Coordinator -&gt; all replicas: liveness signal.
     */
        public record Heartbeat(int coordinatorId, int epoch) implements Serializable {
    }

    /**
     * Token circulating on the ring during a coordinator election. It carries, for
     * every replica it visited, the identifier of the most recent update that
     * replica knows about.
     *
     * @param round         Election attempt: stale attempts are discarded.
     * @param decidedWinner Set once a replica has computed the winner, {@code null} before that.
     */
        public record Election(int crashedCoordinatorId, int round, Map<Integer, UpdateID> candidates,
                               Integer decidedWinner) implements Serializable {
            public Election(int crashedCoordinatorId, int round, Map<Integer, UpdateID> candidates,
                            Integer decidedWinner) {
                this.crashedCoordinatorId = crashedCoordinatorId;
                this.round = round;
                this.candidates = Map.copyOf(candidates);
                this.decidedWinner = decidedWinner;
            }
        }

    /**
     * Acknowledgement of an {@link Election} message, used to detect ring holes.
     */
        public record ElectionAck(int round) implements Serializable {
    }

    /**
         * New coordinator -&gt; all replicas: announces the new leadership and carries
         * the history of the updates the new coordinator knows, so that every replica
         * can apply the ones it is missing.
         */
        public record Synchronization(int newCoordinatorId, int epoch, List<Update> history) implements Serializable {
            public Synchronization(int newCoordinatorId, int epoch, List<Update> history) {
                this.newCoordinatorId = newCoordinatorId;
                this.epoch = epoch;
                this.history = List.copyOf(history);
            }
        }

    // =================================================================================
    // Internal (self-addressed) timeout messages
    // =================================================================================

    private static class HeartbeatTick implements Serializable {
    }

    private static class CoordinatorSilent implements Serializable {
    }

    private record WriteOkTimeout(UpdateID uid) implements Serializable {
    }

    private record ForwardTimeout(long writeId) implements Serializable {
    }

    private record ElectionAckTimeout(int round, int target) implements Serializable {
    }

    private record ElectionTimeout(int round) implements Serializable {
    }

    // =================================================================================
    // Local bookkeeping (not exchanged over the network)
    // =================================================================================

    /** A client write this replica is responsible for answering. */
    private static class PendingWrite {
        final ActorRef client;
        final int index;
        final int value;
        final long clientRequestId;
        Cancellable timeout;

        PendingWrite(ActorRef client, int index, int value, long clientRequestId) {
            this.client = client;
            this.index = index;
            this.value = value;
            this.clientRequestId = clientRequestId;
        }
    }

    // =================================================================================
    // State
    // =================================================================================

    // --- system membership (immutable once initialised) ---
    private Map<Integer, ActorRef> replicas = Collections.emptyMap();
    private List<Integer> ring = Collections.emptyList();
    private int coordinatorId = -1;

    // --- replicated data and update history ---
    private final int[] positions = new int[POSITIONS_LIST_LENGTH];
    private final List<Update> history = new ArrayList<>();
    private final TreeMap<UpdateID, Update> pendingUpdates = new TreeMap<>();
    private UpdateID lastDelivered = UpdateID.NONE;
    private UpdateID lastKnown = UpdateID.NONE;
    private int epoch = 0;

    // --- coordinator side ---
    private int nextSequence = 0;
    private final Deque<Forward> writeQueue = new ArrayDeque<>();
    private Update inFlight = null;
    private final Set<Integer> ackSet = new HashSet<>();
    private Cancellable heartbeatTicker = null;

    // --- non-coordinator side ---
    private Cancellable coordinatorSilentTimer = null;
    private final Map<UpdateID, Cancellable> writeOkTimers = new HashMap<>();
    private final LinkedHashMap<Long, PendingWrite> myWrites = new LinkedHashMap<>();
    private long nextWriteId = 0;

    // --- election ---
    private boolean electing = false;
    private int electionRound = 0;
    private Integer electionStartedFiredFor = null;
    private Election currentElection = null;
    private int ringOffset = 1;
    private int currentTarget = -1;
    private Cancellable electionAckTimer = null;
    private Cancellable electionGlobalTimer = null;

    // --- crash emulation ---
    private boolean crashed = false;
    private Crash crashRule = null;
    private int crashCounter = 0;

    // =================================================================================
    // Construction
    // =================================================================================

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
                   Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
                                          ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    // =================================================================================
    // Abstract methods required by AbstractReplica
    // =================================================================================

    @Override
    public int getSystemNumberOfActors() {
        return replicas.isEmpty() ? 1 : replicas.size();
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.replicas = sysInit.group; // already unmodifiable: no shared mutable state
        List<Integer> ids = new ArrayList<>(sysInit.group.keySet());
        Collections.sort(ids);
        this.ring = Collections.unmodifiableList(ids);
        this.coordinatorId = sysInit.coordinator_id;
        this.epoch = 0;
        this.nextSequence = 0;

        debug("initialised: " + replicas.size() + " replicas, coordinator " + coordinatorId);

        if (isCoordinator()) {
            scheduleHeartbeatTick();
        } else {
            armCoordinatorSilentTimer();
        }
    }

    /**
     * Emulated crash. {@link Crash.Type#Now} stops the replica immediately, the
     * other types arm a rule that fires when the replica has handled the requested
     * number of messages of that type, allowing crashes to be injected at precise
     * points of the protocol (e.g. in the middle of an UPDATE broadcast).
     */
    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if (crashed) {
            return;
        }
        if (how_to_crash.type == Crash.Type.Now) {
            enterCrashedState();
        } else {
            this.crashRule = how_to_crash;
            this.crashCounter = 0;
            debug("armed crash rule: " + how_to_crash.type + " after "
                    + how_to_crash.after_n_messages_of_type + " message(s)");
        }
    }

    // =================================================================================
    // Behaviours
    // =================================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ClientRead.class, this::onClientRead)
                .match(ClientWrite.class, this::onClientWrite)
                .match(Forward.class, this::onForward)
                .match(Update.class, this::onUpdate)
                .match(Ack.class, this::onAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(Synchronization.class, this::onSynchronization)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(CoordinatorSilent.class, this::onCoordinatorSilent)
                .match(WriteOkTimeout.class, this::onWriteOkTimeout)
                .match(ForwardTimeout.class, this::onForwardTimeout)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(ElectionTimeout.class, this::onElectionTimeout)
                .build();
    }

    /** A crashed replica silently drops every incoming message. */
    private Receive crashedReceive() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    // =================================================================================
    // Client requests
    // =================================================================================

    private void onClientRead(ClientRead msg) {
        debug("READ (" + msg.index + ") from " + getSender().path().name());
        send(new ClientReadReply(msg.index, positions[msg.index], id, msg.requestId), getSender());
    }

    private void onClientWrite(ClientWrite msg) {
        long writeId = nextWriteId++;
        PendingWrite pw = new PendingWrite(getSender(), msg.index, msg.value, msg.requestId);
        myWrites.put(writeId, pw);
        debug("WRITE (" + msg.index + ", " + msg.value + ") from " + getSender().path().name());
        forwardWriteToCoordinator(writeId, pw);
    }

    /**
     * Sends a client write to the current coordinator (or enqueues it locally if the
     * coordinator is currently unknown because an election is running) and arms the
     * timeout used to detect a coordinator that never starts the update protocol.
     */
    private void forwardWriteToCoordinator(long writeId, PendingWrite pw) {
        if (electing || coordinatorId < 0) {
            debug("election in progress: write " + writeId + " will be forwarded later");
            return;
        }
        Forward fwd = new Forward(id, writeId, pw.client, pw.index, pw.value);
        if (isCoordinator()) {
            writeQueue.add(fwd);
            startNextUpdate();
        } else {
            send(fwd, replicas.get(coordinatorId));
        }
        cancel(pw.timeout);
        pw.timeout = schedule(forwardTimeoutMs(), new ForwardTimeout(writeId));
    }

    /** Coordinator side: a peer asks to run the update protocol for a client write. */
    private void onForward(Forward msg) {
        if (!isCoordinator()) {
            // Stale request addressed to a previous coordinator: the origin replica
            // will time out and re-forward it, so it can safely be dropped.
            debug("dropping Forward: not the coordinator");
            return;
        }
        writeQueue.add(msg);
        startNextUpdate();
    }

    // =================================================================================
    // Two-phase, quorum-based total order broadcast (coordinator side)
    // =================================================================================

    private int quorum() {
        return (getSystemNumberOfActors() / 2) + 1;
    }

    /**
     * Starts the next update, if any. Updates are broadcast strictly one at a time:
     * this is what makes the sequence numbers, and therefore the delivery order,
     * identical on every replica.
     */
    private void startNextUpdate() {
        if (crashed || !isCoordinator() || electing || inFlight != null) {
            return;
        }
        Forward fwd = writeQueue.poll();
        if (fwd == null) {
            return;
        }
        UpdateID uid = new UpdateID(epoch, nextSequence++);
        Update u = new Update(uid, fwd.index, fwd.value, fwd.originId, fwd.writeId, fwd.client);
        inFlight = u;
        ackSet.clear();
        log("coordinator: broadcasting UPDATE " + uid + " (" + u.index + ", " + u.value + ")");
        broadcast(u, Crash.Type.Update);
    }

    private void onAck(Ack msg) {
        if (crashed || !isCoordinator() || inFlight == null || !inFlight.uid.equals(msg.uid)) {
            return; // late ACK for an already committed update
        }
        ackSet.add(msg.fromId);
        if (ackSet.size() < quorum()) {
            return;
        }
        UpdateID uid = inFlight.uid;
        inFlight = null; // any further ACK for this update is now irrelevant
        ackSet.clear();
        log("coordinator: quorum reached for " + uid + ", broadcasting WRITEOK");
        broadcast(new WriteOk(uid), Crash.Type.WriteOK);
        startNextUpdate();
    }

    // =================================================================================
    // Two-phase, quorum-based total order broadcast (replica side)
    // =================================================================================

    private void onUpdate(Update msg) {
        if (crashed) {
            return;
        }
        if (msg.uid.epoch < epoch || msg.uid.compareTo(lastDelivered) <= 0) {
            return; // stale update from a previous epoch, or already delivered
        }
        pendingUpdates.put(msg.uid, msg);
        if (msg.uid.compareTo(lastKnown) > 0) {
            lastKnown = msg.uid;
        }

        // The coordinator started the protocol: the local write is on its way.
        if (msg.originId == id) {
            PendingWrite pw = myWrites.get(msg.writeId);
            if (pw != null) {
                cancel(pw.timeout);
                pw.timeout = null;
            }
        }

        // The coordinator is alive: refresh the failure detector.
        armCoordinatorSilentTimer();

        // If WRITEOK does not arrive in time, the coordinator is considered crashed.
        cancel(writeOkTimers.remove(msg.uid));
        writeOkTimers.put(msg.uid, schedule(writeOkTimeoutMs(), new WriteOkTimeout(msg.uid)));

        debug("received UPDATE " + msg.uid + ", sending ACK");
        send(new Ack(msg.uid, id), getSender());

        if (!isCoordinator() && crashPointReached(Crash.Type.Update)) {
            enterCrashedState();
        }
    }

    private void onWriteOk(WriteOk msg) {
        if (crashed) {
            return;
        }
        cancel(writeOkTimers.remove(msg.uid));
        armCoordinatorSilentTimer();

        Update u = pendingUpdates.remove(msg.uid);
        if (u != null) {
            applyUpdate(u);
        }

        if (!isCoordinator() && crashPointReached(Crash.Type.WriteOK)) {
            enterCrashedState();
        }
    }

    /**
     * Delivers an update: writes the value, records it in the history and notifies
     * the origin replica's client. Updates already delivered are ignored, so that a
     * value is never applied (nor reported) twice.
     */
    private void applyUpdate(Update u) {
        if (u.uid.compareTo(lastDelivered) <= 0) {
            return;
        }
        positions[u.index] = u.value;
        history.add(u);
        lastDelivered = u.uid;
        if (u.uid.compareTo(lastKnown) > 0) {
            lastKnown = u.uid;
        }
        pendingUpdates.remove(u.uid);

        log("applied update " + u.uid + " (" + u.index + ", " + u.value + ")");
        callbackOnUpdateApplied(u.index, u.value);

        if (u.originId == id) {
            PendingWrite pw = myWrites.remove(u.writeId);
            if (pw != null) {
                cancel(pw.timeout);
                send(new ClientWriteReply(u.index, u.value, id, pw.clientRequestId), pw.client);
            }
        }
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    private void onHeartbeatTick(HeartbeatTick msg) {
        if (crashed || !isCoordinator()) {
            return;
        }
        if (crashPointReached(Crash.Type.Heartbeat)) {
            enterCrashedState();
            return;
        }
        broadcastToOthers(new Heartbeat(id, epoch), null);
        scheduleHeartbeatTick();
    }

    private void scheduleHeartbeatTick() {
        cancel(heartbeatTicker);
        heartbeatTicker = schedule(getCoordinatorBeatInterval(), new HeartbeatTick());
    }

    private void onHeartbeat(Heartbeat msg) {
        if (crashed || msg.epoch < epoch) {
            return;
        }
        if (!electing) {
            coordinatorId = msg.coordinatorId;
        }
        armCoordinatorSilentTimer();
        if (crashPointReached(Crash.Type.Heartbeat)) {
            enterCrashedState();
        }
    }

    /** (Re)arms the timer that fires when the coordinator has been silent too long. */
    private void armCoordinatorSilentTimer() {
        if (crashed || isCoordinator()) {
            return;
        }
        cancel(coordinatorSilentTimer);
        coordinatorSilentTimer = schedule(coordinatorSilenceTimeoutMs(), new CoordinatorSilent());
    }

    private void onCoordinatorSilent(CoordinatorSilent msg) {
        if (crashed || isCoordinator() || electing) {
            return;
        }
        log("coordinator " + coordinatorId + " is silent: starting an election");
        startElection(coordinatorId);
    }

    private void onWriteOkTimeout(WriteOkTimeout msg) {
        if (crashed || electing || !pendingUpdates.containsKey(msg.uid)) {
            return;
        }
        log("no WRITEOK for " + msg.uid + ": coordinator " + coordinatorId + " considered crashed");
        startElection(coordinatorId);
    }

    private void onForwardTimeout(ForwardTimeout msg) {
        PendingWrite pw = myWrites.get(msg.writeId);
        if (crashed || electing || pw == null) {
            return;
        }
        log("coordinator " + coordinatorId + " did not start the update protocol: starting an election");
        startElection(coordinatorId);
    }

    // =================================================================================
    // Ring-based coordinator election
    // =================================================================================

    private void startElection(int crashedCoordinatorId) {
        if (crashed) {
            return;
        }
        electionRound++;
        enterElectionState(crashedCoordinatorId);

        Map<Integer, UpdateID> candidates = new HashMap<>();
        candidates.put(id, lastKnown);
        Election msg = new Election(crashedCoordinatorId, electionRound, candidates, null);
        fireElectionStartedOnce(crashedCoordinatorId);
        forwardElection(msg);
    }

    /**
     * Freezes the normal activity of the replica for the duration of the election:
     * no new update is started and the "most recent known update" advertised on the
     * ring cannot change while the token is circulating.
     */
    private void enterElectionState(int crashedCoordinatorId) {
        electing = true;
        coordinatorId = -1;
        inFlight = null;
        ackSet.clear();
        cancel(coordinatorSilentTimer);
        coordinatorSilentTimer = null;
        cancel(heartbeatTicker);
        heartbeatTicker = null;
        for (Cancellable c : writeOkTimers.values()) {
            cancel(c);
        }
        writeOkTimers.clear();
        for (PendingWrite pw : myWrites.values()) {
            cancel(pw.timeout);
            pw.timeout = null;
        }
        cancel(electionGlobalTimer);
        electionGlobalTimer = schedule(electionGlobalTimeoutMs(), new ElectionTimeout(electionRound));
    }

    /** The API requires this callback to be invoked at most once per election. */
    private void fireElectionStartedOnce(int crashedCoordinatorId) {
        if (electionStartedFiredFor != null && electionStartedFiredFor == crashedCoordinatorId) {
            return;
        }
        electionStartedFiredFor = crashedCoordinatorId;
        callbackOnElectionStarted(crashedCoordinatorId);
    }

    private void onElection(Election msg) {
        if (crashed) {
            return;
        }
        // Always acknowledge: the sender uses the ACK to tell whether we are alive.
        send(new ElectionAck(msg.round), getSender());

        // Several replicas may detect the crash and start their own token. Once the
        // election has been resolved, the tokens still travelling on the ring belong
        // to a coordinator that has already been replaced and are simply dropped, so
        // that a single new coordinator (and a single new epoch) is installed.
        if (!electing && coordinatorId >= 0 && msg.crashedCoordinatorId != coordinatorId) {
            debug("dropping the token of an election that is already over");
            return;
        }

        if (msg.round < electionRound) {
            return; // token of an obsolete election attempt
        }
        if (msg.round > electionRound) {
            electionRound = msg.round;
            enterElectionState(msg.crashedCoordinatorId);
        } else if (!electing) {
            enterElectionState(msg.crashedCoordinatorId);
        }

        if (!msg.candidates.containsKey(id)) {
            // First time we see this token: add our own most recent update and pass it on.
            Map<Integer, UpdateID> candidates = new HashMap<>(msg.candidates);
            candidates.put(id, lastKnown);
            fireElectionStartedOnce(msg.crashedCoordinatorId);
            forwardElection(new Election(msg.crashedCoordinatorId, msg.round, candidates, msg.decidedWinner));
            return;
        }

        // The token completed a full loop: every reachable replica is in it, so all
        // replicas would compute the same winner from the same information.
        int winner = computeWinner(msg.candidates);
        if (winner == id) {
            becomeCoordinator(msg.candidates);
        } else {
            forwardElection(new Election(msg.crashedCoordinatorId, msg.round, msg.candidates, winner));
        }
    }

    /** Most recent update wins; ties are broken with the (unique) replica id. */
    private int computeWinner(Map<Integer, UpdateID> candidates) {
        int winner = -1;
        UpdateID best = null;
        for (Map.Entry<Integer, UpdateID> e : candidates.entrySet()) {
            int cmp = (best == null) ? 1 : e.getValue().compareTo(best);
            if (cmp > 0 || (cmp == 0 && e.getKey() > winner)) {
                best = e.getValue();
                winner = e.getKey();
            }
        }
        return winner;
    }

    private void forwardElection(Election msg) {
        currentElection = msg;
        ringOffset = 1;
        sendElectionToNext();
    }

    /**
     * Sends the token to the next replica on the ring, skipping the coordinator we
     * already know to be crashed, and arms the ACK timeout used to skip replicas
     * that crashed during the election itself.
     */
    private void sendElectionToNext() {
        if (crashed || currentElection == null) {
            return;
        }
        while (ringOffset < ring.size()) {
            int target = ringSuccessor(ringOffset);
            if (target != currentElection.crashedCoordinatorId) {
                currentTarget = target;
                cancel(electionAckTimer);
                if (crashPointReached(Crash.Type.Election)) {
                    enterCrashedState();
                    return;
                }
                debug("forwarding ELECTION (round " + currentElection.round + ") to replica " + target);
                send(currentElection, replicas.get(target));
                electionAckTimer = schedule(electionAckTimeoutMs(),
                        new ElectionAckTimeout(currentElection.round, target));
                return;
            }
            ringOffset++;
        }
        // Every other replica appears to be unreachable: we are the only survivor.
        becomeCoordinator(currentElection.candidates);
    }

    private int ringSuccessor(int offset) {
        int myPosition = ring.indexOf(id);
        return ring.get((myPosition + offset) % ring.size());
    }

    private void onElectionAck(ElectionAck msg) {
        if (crashed || msg.round != electionRound) {
            return;
        }
        cancel(electionAckTimer);
        electionAckTimer = null;
    }

    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        if (crashed || !electing || msg.round != electionRound || msg.target != currentTarget) {
            return;
        }
        debug("replica " + msg.target + " did not acknowledge the token: skipping it");
        ringOffset++;
        sendElectionToNext();
    }

    /**
     * Guarantees termination: if the token is lost (for instance because the winner
     * crashed just after being chosen) the election is restarted from scratch. The
     * crashed replicas are skipped in the new round, so a different winner emerges.
     */
    private void onElectionTimeout(ElectionTimeout msg) {
        if (crashed || !electing || msg.round != electionRound) {
            return;
        }
        int crashedCoordinator = (currentElection != null)
                ? currentElection.crashedCoordinatorId
                : coordinatorId;
        log("election round " + msg.round + " did not terminate: starting a new round");
        startElection(crashedCoordinator);
    }

    /**
     * Takes over as coordinator. Before opening the new epoch, every update that was
     * left incomplete by the crashed coordinator is delivered: this is what
     * preserves uniform agreement, since an update acknowledged by a quorum is
     * necessarily known to the most up-to-date replica, which is precisely the
     * winner of the election.
     */
    private void becomeCoordinator(Map<Integer, UpdateID> candidates) {
        if (crashed) {
            return;
        }
        cancel(electionAckTimer);
        electionAckTimer = null;
        cancel(electionGlobalTimer);
        electionGlobalTimer = null;
        currentElection = null;
        electing = false;
        coordinatorId = id;

        // Complete the interrupted broadcasts, in total order.
        for (Update u : new ArrayList<>(pendingUpdates.values())) {
            applyUpdate(u);
        }
        pendingUpdates.clear();

        int maxEpoch = Math.max(epoch, lastKnown.epoch);
        for (UpdateID uid : candidates.values()) {
            maxEpoch = Math.max(maxEpoch, uid.epoch);
        }
        epoch = maxEpoch + 1;
        nextSequence = 0;

        log("elected as the new coordinator, starting epoch " + epoch);
        callbackOnCoordinatorElected(id);

        broadcastToOthers(new Synchronization(id, epoch, history), null);
        scheduleHeartbeatTick();

        // Client writes that never became an update are re-issued in the new epoch.
        reissuePendingWrites();
        startNextUpdate();
    }

    private void onSynchronization(Synchronization msg) {
        if (crashed || msg.epoch < epoch) {
            return;
        }
        if (msg.epoch == epoch && !electing && coordinatorId == msg.newCoordinatorId) {
            return; // duplicate announcement of a coordinator we already follow
        }
        cancel(electionAckTimer);
        electionAckTimer = null;
        cancel(electionGlobalTimer);
        electionGlobalTimer = null;
        currentElection = null;
        electing = false;
        coordinatorId = msg.newCoordinatorId;
        epoch = msg.epoch;
        nextSequence = 0;

        // Apply everything we are missing, in the coordinator's order.
        for (Update u : msg.history) {
            applyUpdate(u);
        }
        // Updates the new coordinator does not know about were never committed.
        pendingUpdates.clear();
        lastKnown = lastDelivered;

        log("synchronised with the new coordinator " + msg.newCoordinatorId + " (epoch " + epoch + ")");
        callbackOnCoordinatorElected(msg.newCoordinatorId);

        armCoordinatorSilentTimer();
        reissuePendingWrites();
    }

    /** Re-sends to the new coordinator the client writes that are still unanswered. */
    private void reissuePendingWrites() {
        for (Map.Entry<Long, PendingWrite> e : new ArrayList<>(myWrites.entrySet())) {
            forwardWriteToCoordinator(e.getKey(), e.getValue());
        }
    }

    // =================================================================================
    // Crash emulation helpers
    // =================================================================================

    /**
     * Consumes one unit of the armed crash rule and tells whether the replica must
     * stop right now. Counting individual transmissions (rather than protocol
     * rounds) is what allows a crash to be injected in the middle of a broadcast.
     */
    private boolean crashPointReached(Crash.Type type) {
        if (crashed) {
            return true;
        }
        if (crashRule == null || crashRule.type != type) {
            return false;
        }
        if (crashCounter >= crashRule.after_n_messages_of_type) {
            return true;
        }
        crashCounter++;
        return false;
    }

    private void enterCrashedState() {
        if (crashed) {
            return;
        }
        crashed = true;
        cancel(heartbeatTicker);
        cancel(coordinatorSilentTimer);
        cancel(electionAckTimer);
        cancel(electionGlobalTimer);
        for (Cancellable c : writeOkTimers.values()) {
            cancel(c);
        }
        writeOkTimers.clear();
        for (PendingWrite pw : myWrites.values()) {
            cancel(pw.timeout);
        }
        heartbeatTicker = null;
        coordinatorSilentTimer = null;
        electionAckTimer = null;
        electionGlobalTimer = null;
        getContext().become(crashedReceive());
    }

    // =================================================================================
    // Communication helpers
    // =================================================================================

    private boolean isCoordinator() {
        return coordinatorId == id;
    }

    /** Point-to-point send through the emulated (reliable, FIFO) network channel. */
    private void send(Serializable m, ActorRef dst) {
        if (crashed || dst == null) {
            return;
        }
        tell(m, dst);
    }

    /** Sends to every replica, this one included. */
    private void broadcast(Serializable m, Crash.Type crashPoint) {
        for (int rid : ring) {
            if (crashed) {
                return;
            }
            if (crashPoint != null && crashPointReached(crashPoint)) {
                log("crashing in the middle of a broadcast");
                enterCrashedState();
                return;
            }
            send(m, replicas.get(rid));
        }
    }

    /** Sends to every replica except this one. */
    private void broadcastToOthers(Serializable m, Crash.Type crashPoint) {
        for (int rid : ring) {
            if (crashed) {
                return;
            }
            if (rid == id) {
                continue;
            }
            if (crashPoint != null && crashPointReached(crashPoint)) {
                log("crashing in the middle of a broadcast");
                enterCrashedState();
                return;
            }
            send(m, replicas.get(rid));
        }
    }

    // =================================================================================
    // Timers
    // =================================================================================

    private Cancellable schedule(long delayMillis, Serializable msg) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                msg,
                getContext().system().dispatcher(),
                getSelf());
    }

    private void cancel(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }

    /** One network hop, generously padded with the tolerance of the base class. */
    private long hop() {
        return getMaxLatencyPlusTolerance();
    }

    /** UPDATE -> ACK -> WRITEOK, i.e. three hops plus scheduling slack. */
    private long writeOkTimeoutMs() {
        return 4 * hop() + 100;
    }

    /** Forward -> UPDATE, plus the time the coordinator may spend on earlier writes. */
    private long forwardTimeoutMs() {
        return 6 * hop() + 100;
    }

    /** Must stay below three heartbeat intervals, as assumed by the test harness. */
    private long coordinatorSilenceTimeoutMs() {
        return 2L * getCoordinatorBeatInterval() + 2 * hop();
    }

    private long electionAckTimeoutMs() {
        return 2 * hop() + 50;
    }

    /** Enough for a full ring circulation, including a few skipped replicas. */
    private long electionGlobalTimeoutMs() {
        return 2L * getSystemNumberOfActors() * electionAckTimeoutMs() + 500;
    }
}