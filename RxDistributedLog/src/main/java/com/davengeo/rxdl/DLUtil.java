package com.davengeo.rxdl;

import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.FutureEventListener;
import io.reactivex.FlowableEmitter;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.twitter.util.Duration.fromTimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Collection of actions which involve the distributed log.
 * Currently fetch the firstDLSN and fetch the lastDLSN in a functional way.
 *
 */
@Slf4j
public class DLUtil {

    /**
     * Try to fetch the first DLSN of the indicated distributed log.
     * When there is any error it returns invalidDLSN.
     *
     * @param logManager manager of the log to use
     * @return the first dlsn or invalid if any error.
     */
    public static DLSN firstDlsn(@NonNull DistributedLogManager logManager) {
        return Try
                .of(() -> FutureUtils.result(logManager.getFirstDLSNAsync()))
                .recover(LogEmptyException.class, DLSN.InitialDLSN)
                .getOrElseGet(throwable -> {
                    log.error("Found error {} when accessing log {}", throwable.getMessage(), logManager.getStreamName());
                    return DLSN.InvalidDLSN;
                });
    }

    /**
     * Try to fetch the last DLSN of the indicated distributed log.
     * When there is any error it returns invalidDLSN.
     *
     * @param logManager manager of the log to use
     * @return the last dlsn or invalid if any error.
     */
    public static DLSN lastDlsn(@NonNull DistributedLogManager logManager) {
        return Try
                .of(logManager::getLastDLSN)
                .recover(LogEmptyException.class, DLSN.InitialDLSN)
                .getOrElseGet(throwable -> {
                    log.error("Found error {} when accessing log {}", throwable.getMessage(), logManager.getStreamName());
                    return DLSN.InvalidDLSN;
                });
    }

}
