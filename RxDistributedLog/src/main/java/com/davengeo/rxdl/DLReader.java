package com.davengeo.rxdl;

import io.reactivex.Flowable;
import io.vavr.control.Either;

import java.nio.ByteBuffer;

/**
 * This is the common interface for distributed log reading artifacts.
 * The interface express an auto-closeable resource which have a tail.
 * Retrieving the tail and subscribe to the it is basically to tail the log.
 *
 */
public interface DLReader<T> extends AutoCloseable {
    /**
     * Gets the tail of the reader artifact to proceed with subscription of it.
     * It returns a RxJava 2.0 artifact flowable.
     *
     * @return either the observable -flowable- of the emission of this log or an exception.
     */
    Either<Throwable, Flowable<T>> getTail();
}
