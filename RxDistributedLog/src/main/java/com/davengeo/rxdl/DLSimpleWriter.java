package com.davengeo.rxdl;

import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.util.FutureUtils;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.twitter.util.Duration.fromTimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This is a simple writer for distributed log.
 * It uses an encapsulated logManager to create ephemeral append only writers.
 * Those writers are closed and consumed as soon as they are used.
 *
 */
@Slf4j
public class DLSimpleWriter implements DLWriter {

    private final DistributedLogManager logManager;

    /**
     * it creates a new DLSimpleWriter encapsulating a log Manager.
     *
     * @param logManager the log manager to use with this DLSimpleWriter.
     */
    public DLSimpleWriter(@NonNull DistributedLogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * it writes some content into the distributed log.
     * To do that it gets a new appendOnlyAsyncWriter from the manager and it closes it after the writing.
     *
     * @param content content to write in the log
     * @return either the DLSN of the log after writing or an exception.
     */
    @Override
    public Either<Throwable, DLSN> write(@NonNull ByteBuffer content) {
        final AtomicReference<DLSN> result = new AtomicReference<>();
        final AtomicReference<AppendOnlyStreamWriter> writer = new AtomicReference<>();
        return Try
                .of(logManager::getAppendOnlyStreamWriter)
                .peek(writer::set)
                .andThenTry(streamWriter -> result.set(
                        FutureUtils.result(streamWriter.write(content.array()), fromTimeUnit(5, TimeUnit.SECONDS))))
                .andFinallyTry(() -> {
                    if (!Objects.isNull(writer.get())) {
                        writer.get().close();
                    }
                })
                .onFailure(t -> log.error("Found {} when trying to write {} into log {}",
                        new Object[]{t.getMessage(), new String(content.array(), UTF_8), logManager.getStreamName()}
                ))
                .toEither()
                .map(i -> result.get());
    }

    /**
     * it closes the encapsulated log manager.
     * Thus the log Manager and this writer are not reusable any more after this action.
     */
    @Override
    public void close() {
        try {
            logManager.close();
        } catch (IOException e) {
            log.error("Error {} {} caused by {}", new Object[] {
                    e.getClass().getName(), e.getMessage(), e.getCause()});
        }
    }
}
