package com.davengeo.rxdl;


import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.FutureEventListener;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.schedulers.Schedulers;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static io.reactivex.BackpressureStrategy.BUFFER;

/**
 * A simple tail that implements the DLReader interface.
 * it reads a log by using the log manager from a given DLSN position.
 */
@Slf4j
public class DLTail implements DLReader<EmissionWithOrigin> {

    private final DistributedLogManager logManager;
    private final DLSN dlsn;
    private AsyncLogReader reader;
    private Function<String, FutureEventListener<LogRecordWithDLSN>> readListener;

    /**
     * DLTail constructor
     * <p>
     * it builds a new DLTail instance, the instance is not ready until getTail()
     *
     * @param logManager The Distributed log manager to use
     * @param dlsn       The DLSN from where start to read
     */
    public DLTail(@NonNull DistributedLogManager logManager,
                  @NonNull DLSN dlsn) {
        this.logManager = logManager;
        this.dlsn = dlsn;
    }

    /**
     * Try to get the observable (Flowable) attached to this reader.
     *
     * @return either bytebuffer flowable or an exception
     */
    public Either<Throwable, Flowable<EmissionWithOrigin>> getTail() {
        if (Objects.equals(dlsn, DLSN.InvalidDLSN)) {
            return Either.left(new IllegalArgumentException(String.format("initial DLSN to read is invalid in log %s",
                    logManager.getStreamName())));
        } else {
            return fabric(logManager, dlsn);
        }
    }

    private Either<Throwable, Flowable<EmissionWithOrigin>> fabric(DistributedLogManager logManager, DLSN dlsn) {
        return Try
                .of(() -> FutureUtils.result(logManager.openAsyncLogReader(dlsn)))
                .onFailure(t -> log.error("Error {} when trying to open reader of log {}",
                        t.getMessage(), logManager.getStreamName()))
                .peek(asyncLogReader -> this.reader = asyncLogReader)
                .map(reader -> Flowable.create(this::subscribing, BUFFER).observeOn(Schedulers.computation()))
                .toEither();
    }

    private void subscribing(FlowableEmitter<EmissionWithOrigin> emitter) {
        this.readListener = ListenerFactory.readListener(emitter, this::tailNext);
        this.tailNext();
    }

    private void tailNext() {
        this.reader.readNext().addEventListener(readListener.apply(reader.getStreamName()));
    }

    /**
     * It closes the reader so there are no more events to emit.
     * After closing this reader cannot be reused anymore.
     * The encapsulated log manager might be reused though
     */
    public void close() {
        Optional.ofNullable(reader)
                .ifPresent(asyncLogReader ->
                        Try.of(() -> FutureUtils.result(asyncLogReader.asyncClose())));
    }


}
