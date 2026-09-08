package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Demonstration scenarios.
 *
 * <p>
 * Run every scenario with {@code ./gradlew run}, or a single one with
 * {@code ./gradlew run --args="2"}.
 * </p>
 */
public class Main {

    private static final int N_REPLICAS = 5;
    private static final int COORDINATOR_ID = 0;

    /** Holds the actors of one scenario. */
    private static class Deployment {
        ActorSystem system;
        Map<Integer, ActorRef> replicas;
        List<ActorRef> clients = new ArrayList<>();
    }

    // =================================================================================
    // Set-up helpers
    // =================================================================================

    private static Deployment deploy(String name, int nReplicas, int coordinatorId) {
        Deployment d = new Deployment();
        d.system = ActorSystem.create(name);
        d.replicas = new HashMap<>(nReplicas);
        for (int i = 0; i < nReplicas; i++) {
            d.replicas.put(i, d.system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i));
        }
        InitSystem init = new InitSystem(d.replicas, coordinatorId);
        for (ActorRef r : d.replicas.values()) {
            r.tell(init, ActorRef.noSender());
        }
        return d;
    }

    private static ActorRef newClient(Deployment d, String name, int targetReplica, int nReplicas) {
        long readTimeout = (long) AbstractReplica.MAX_LATENCY * nReplicas * 8;
        long writeTimeout = readTimeout + 5L * AbstractReplica.COORDINATOR_BEAT_INTERVAL;
        ActorRef c = d.system.actorOf(
                Client.props(readTimeout, writeTimeout, Optional.of(d.replicas.get(targetReplica))),
                name);
        d.clients.add(c);
        return c;
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void banner(String title) {
        Logger.log("======================================================================");
        Logger.log("  " + title);
        Logger.log("======================================================================");
    }

    private static void shutdown(Deployment d) {
        pause(500);
        d.system.terminate();
        pause(300);
    }

    // =================================================================================
    // Scenario 1 - normal operation
    // =================================================================================

    /**
     * No failures: two clients attached to different replicas issue writes and
     * reads. All replicas apply the updates in the same total order.
     */
    private static void scenarioNormalOperation() {
        banner("SCENARIO 1 - normal operation, no crashes");
        Deployment d = deploy("scenario1", N_REPLICAS, COORDINATOR_ID);
        pause(200);

        ActorRef writer = newClient(d, "writer", 3, N_REPLICAS);
        ActorRef reader = newClient(d, "reader", 1, N_REPLICAS);

        for (int v = 1; v <= 3; v++) {
            writer.tell(new AbstractClient.WriteRequest(0, v * 100), ActorRef.noSender());
            pause(250);
            reader.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
            pause(150);
        }

        // A write on a different position of the list.
        writer.tell(new AbstractClient.WriteRequest(7, 555), ActorRef.noSender());
        pause(300);
        reader.tell(new AbstractClient.ReadRequest(7), ActorRef.noSender());
        pause(300);

        shutdown(d);
    }

    // =================================================================================
    // Scenario 2 - a minority of replicas crashes
    // =================================================================================

    /**
     * Two out of five replicas crash. A quorum (3) is still available, so updates
     * keep being committed without any election.
     */
    private static void scenarioMinorityCrash() {
        banner("SCENARIO 2 - a minority of replicas crashes, the quorum survives");
        Deployment d = deploy("scenario2", N_REPLICAS, COORDINATOR_ID);
        pause(200);

        ActorRef client = newClient(d, "client", 4, N_REPLICAS);

        Logger.log(">>> crashing replicas 1 and 2 (quorum is 3 out of 5)");
        d.replicas.get(1).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        d.replicas.get(2).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        pause(200);

        client.tell(new AbstractClient.WriteRequest(0, 111), ActorRef.noSender());
        pause(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        pause(400);

        shutdown(d);
    }

    // =================================================================================
    // Scenario 3 - coordinator crash and election
    // =================================================================================

    /**
     * The coordinator crashes after a committed update. The surviving replicas run
     * the ring election, the most up-to-date one takes over and opens a new epoch,
     * and the pending client write is served in that new epoch.
     */
    private static void scenarioCoordinatorCrash() {
        banner("SCENARIO 3 - the coordinator crashes, a new one is elected");
        Deployment d = deploy("scenario3", N_REPLICAS, COORDINATOR_ID);
        pause(200);

        ActorRef client = newClient(d, "client", 4, N_REPLICAS);

        client.tell(new AbstractClient.WriteRequest(0, 10), ActorRef.noSender());
        pause(400);

        Logger.log(">>> crashing the coordinator (replica " + COORDINATOR_ID + ")");
        d.replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        pause(100);

        // Issued while there is no coordinator: it is served once the election ends.
        client.tell(new AbstractClient.WriteRequest(0, 20), ActorRef.noSender());

        pause(6000);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        pause(500);

        shutdown(d);
    }

    // =================================================================================
    // Scenario 4 - crash in the middle of a broadcast
    // =================================================================================

    /**
     * The coordinator crashes after having sent WRITEOK to part of the group: some
     * replicas delivered the update, the others did not. The new coordinator must
     * complete the interrupted broadcast, so that every correct replica converges to
     * the same history (uniform agreement).
     */
    private static void scenarioCrashDuringBroadcast() {
        banner("SCENARIO 4 - the coordinator crashes in the middle of the WRITEOK broadcast");
        Deployment d = deploy("scenario4", N_REPLICAS, COORDINATOR_ID);
        pause(200);

        ActorRef client = newClient(d, "client", 4, N_REPLICAS);

        Logger.log(">>> the coordinator will stop after sending only 2 WRITEOK messages");
        d.replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.WriteOK, 2), ActorRef.noSender());
        pause(100);

        client.tell(new AbstractClient.WriteRequest(0, 999), ActorRef.noSender());

        pause(8000);
        Logger.log(">>> reading from every surviving replica: they must all agree");
        for (int i = 1; i < N_REPLICAS; i++) {
            ActorRef c = newClient(d, "check_" + i, i, N_REPLICAS);
            c.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
            pause(150);
        }
        pause(500);

        shutdown(d);
    }

    // =================================================================================

    public static void main(String[] args) {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        int only = -1;
        if (args.length > 0) {
            try {
                only = Integer.parseInt(args[0].trim());
            } catch (NumberFormatException ignored) {
            }
        }

        if (only == -1 || only == 1) {
            scenarioNormalOperation();
        }
        if (only == -1 || only == 2) {
            scenarioMinorityCrash();
        }
        if (only == -1 || only == 3) {
            scenarioCoordinatorCrash();
        }
        if (only == -1 || only == 4) {
            scenarioCrashDuringBroadcast();
        }

        banner("DONE");
        System.exit(0);
    }
}