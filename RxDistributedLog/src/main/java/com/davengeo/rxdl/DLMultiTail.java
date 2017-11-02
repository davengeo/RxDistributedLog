package com.davengeo.rxdl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.schedulers.Schedulers;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.reactivex.BackpressureStrategy.BUFFER;

/**
 * DLMultiTail Distributed log multi reader which can aggregate different sources dynamically.
 * The aggregation is made through the method addSources and can be revoked using deleteSources.
 * The aggregation stream honors the individual time of emission.
 *
 */
@Slf4j
public class DLMultiTail implements DLReader<EmissionWithOrigin> {

    private final Map<String, AsyncLogReader> readers = Maps.newHashMap();
    private final Map<String, Future<LogRecordWithDLSN>> futures = Maps.newHashMap();
    private Function<String, FutureEventListener<LogRecordWithDLSN>> eventListener;

    private final DistributedLogManager logManager;
    private final DLSN dlsn;

    /**
     * Constructor for DLMultiTail. It creates a multi tail with one source -log, stream- associated.
     *
     * @param logManager log manager of the source to include
     * @param dlsn       position from where the log is going to be read
     */
    public DLMultiTail(DistributedLogManager logManager, DLSN dlsn) {
        this.logManager = logManager;
        this.dlsn = dlsn;
    }

    private Try<AsyncLogReader> getAsyncLogReader(DistributedLogManager logManager, DLSN dlsn) {
        return Try.of(() -> logManager.getAsyncLogReader(dlsn));
    }

    /**
     * Add a source -log, stream- to this multi reader so from this moment the emissions of this source
     * is added to the emission of the multi reader honoring the time when they are emitted.
     *
     * @param logManager log manager of the new source to include.
     * @param dlsn       position from where to read the source.
     * @return Optional with an exception if there is any.
     */
    public Optional<Throwable> addSources(@NonNull DistributedLogManager logManager,
                                          @NonNull DLSN dlsn) {
        validateParams(logManager, dlsn);
        return getAsyncLogReader(logManager, dlsn)
                .andThenTry(reader -> {
                    final String origin = logManager.getStreamName();
                    this.futures.put(origin, reader.readNext().addEventListener(eventListener.apply(origin)));
                    this.readers.put(origin, reader);
                })
                .toEither()
                .swap()
                .toJavaOptional();
    }

    private void validateParams(DistributedLogManager manager, DLSN dlsn) {
        Preconditions.checkState(!dlsn.equals(DLSN.InvalidDLSN));
    }

    /**
     * Delete the following source by using the name of it -streamName-. From this moment the emissions of
     * this sources are not going to be part of the emissions of the multi-reader.
     *
     * @param streamName the stream name of the source to delete.
     */
    public void deleteSources(@NonNull String streamName) {
        Optional.ofNullable(futures.get(streamName))
                .ifPresent(Future::cancel);
        readers.remove(streamName);
    }

    /**
     * It fetches the tail of the multi-reader and prepares it to be subscribed.
     *
     * @return either the observable -flowable- associated to the emissions of this multi-reader or
     * an exception.
     */
    public Either<Throwable, Flowable<EmissionWithOrigin>> getTail() {
        return getAsyncLogReader(this.logManager, this.dlsn)
                .andThenTry(reader -> this.readers.put(logManager.getStreamName(), reader))
                .map(asyncLogReader ->
                        Flowable.create(this::subscribing, BUFFER)
                                .observeOn(Schedulers.computation())
                )
                .toEither();
    }

    private void subscribing(FlowableEmitter<EmissionWithOrigin> emitter) {
        this.eventListener = ListenerFactory.readListener(emitter, this::tailNext);
        this.tailNext();
    }

    /**
     * It closes all the readers encapsulated at this moment with the multi-reader, and emits a completion signal.
     * After this moment the multi-reader is not reusable any more.
     */
    public void close() {
        this
                .readers
                .forEach((s, reader) -> Try.of(() -> {
                    log.info("closing multi-reader, stream {} is about to close", s);
                    return FutureUtils.result(reader.asyncClose());
                }));
    }

    private void tailNext() {
        this.readers.forEach((origin, reader) ->
                futures.put(origin, reader.readNext()
                        .addEventListener(eventListener.apply(origin))));
    }
}
