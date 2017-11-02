package com.davengeo.rxdl;

import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.util.FutureUtils;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.twitter.util.Duration.fromTimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class Utils {

    /**
     * Try to write some information into a distributed log.
     * The log is closed automatically after this action.
     *
     * @param logManager the log manager of the distributed log to write
     * @param content    the bytebuffer wrapping the content to write
     * @return either the DLSN writen or an exception.
     */
    public static Either<Throwable, DLSN> write(@NonNull DistributedLogManager logManager,
                                                @NonNull ByteBuffer content) {
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
}
