package com.davengeo.rxdl;

import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.util.FutureEventListener;
import io.reactivex.FlowableEmitter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.function.Function;

@Slf4j
class ListenerFactory {

    static Function<String, FutureEventListener<LogRecordWithDLSN>> readListener(
            FlowableEmitter<EmissionWithOrigin> emitter,
            Runnable tailNext) {

        return origin -> new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof ReadCancelledException) {
                    log.warn("Closing => {}:{}", cause.getMessage(), cause.getCause());
                    emitter.onComplete();
                } else if (cause instanceof DLInterruptedException) {
                    log.warn("Omitting => {}:{}", cause.getMessage(), cause.getCause());
                } else {
                    emitter.onError(cause);
                }
            }

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                try {
                    ListenerFactory.logWhenDebug(record);
                    emitter.onNext(new EmissionWithOrigin(ByteBuffer.wrap(record.getPayload()), origin));
                    tailNext.run();
                } catch (Exception e) {
                    emitter.onError(e);
                }
            }
        };
    }

    private static void logWhenDebug(LogRecordWithDLSN record) {
        if (log.isDebugEnabled()) {
            log.debug("Recorded: txId:{} Sgmt:{} EntryId:{} SlotId:{}", new Object[]{
                    record.getTransactionId(), record.getDlsn().getLogSegmentSequenceNo(),
                    record.getDlsn().getEntryId(), record.getDlsn().getSlotId()
            });
        }
    }
}
