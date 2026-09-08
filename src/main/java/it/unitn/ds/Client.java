package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * An external client of the storage system.
 *
 * <p>
 * A client contacts one replica at a time and waits for the answer. Every
 * request carries a locally unique identifier, so that several requests may be
 * outstanding at the same time and each answer (or timeout) is matched with the
 * request that produced it.
 * </p>
 */
public class Client extends AbstractClient {

    /**
     * Self-addressed message used to detect an unanswered request.
     */
        private record RequestTimeout(long requestId) implements Serializable {
    }

    /** Bookkeeping for an outstanding request. */
    private static class PendingRequest {
        final ActorRef replica;
        final int index;
        final int value;
        final boolean isWrite;
        Cancellable timeout;

        PendingRequest(ActorRef replica, int index, int value, boolean isWrite) {
            this.replica = replica;
            this.index = index;
            this.value = value;
            this.isWrite = isWrite;
        }
    }

    private final Map<Long, PendingRequest> pending = new HashMap<>();
    private long nextRequestId = 0;

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
           Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay,
                              Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
                                          Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
                        Optional.ofNullable(listener)));
    }

    // =================================================================================
    // Sending requests
    // =================================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
        long requestId = nextRequestId++;
        PendingRequest req = new PendingRequest(replica, index, 0, false);
        pending.put(requestId, req);

        log("requesting READ (" + index + ") to " + replica.path().name());
        replica.tell(new Replica.ClientRead(index, requestId), getSelf());
        req.timeout = schedule(getReadTimeoutDelay(), new RequestTimeout(requestId));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        long requestId = nextRequestId++;
        PendingRequest req = new PendingRequest(replica, index, value, true);
        pending.put(requestId, req);

        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
        replica.tell(new Replica.ClientWrite(index, value, requestId), getSelf());
        req.timeout = schedule(getWriteTimeoutDelay(), new RequestTimeout(requestId));
    }

    // =================================================================================
    // Receiving answers
    // =================================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Replica.ClientReadReply.class, this::onReadReply)
                .match(Replica.ClientWriteReply.class, this::onWriteReply)
                .match(RequestTimeout.class, this::onRequestTimeout)
                .build();
    }

    private void onReadReply(Replica.ClientReadReply msg) {
        PendingRequest req = pending.remove(msg.requestId());
        if (req == null) {
            return; // already timed out
        }
        cancel(req.timeout);
        callbackOnReadResult(new ReadResult(true, msg.index(), msg.value(), msg.replicaId()));
    }

    private void onWriteReply(Replica.ClientWriteReply msg) {
        PendingRequest req = pending.remove(msg.requestId());
        if (req == null) {
            return; // already timed out
        }
        cancel(req.timeout);
        callbackOnWriteResult(new WriteResult(true, msg.index(), msg.value(), msg.replicaId()));
    }

    private void onRequestTimeout(RequestTimeout msg) {
        PendingRequest req = pending.remove(msg.requestId);
        if (req == null) {
            return; // the answer arrived in time
        }
        if (req.isWrite) {
            callbackOnWriteTimeout(new WriteTimeout(getSelf(), req.replica, req.index, req.value));
        } else {
            callbackOnReadTimeout(new ReadTimeout(getSelf(), req.replica, req.index));
        }
    }

    // =================================================================================
    // Helpers
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
}