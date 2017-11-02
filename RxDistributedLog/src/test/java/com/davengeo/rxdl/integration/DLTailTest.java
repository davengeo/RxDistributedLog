package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.*;
import com.google.common.collect.Lists;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.util.CountDownLatch;
import com.twitter.util.Future;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vavr.control.Either;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Slf4j
public class DLTailTest {

    private DistributedLogNamespace logNamespace;

    private static final String[] LOG_NAMES = {
            "read-hand-1", "read-hand-2",
            "read-hand-3", "read-hand-4"
    };

    @Before
    public void setup() throws IOException {
        this.logNamespace = DLWithSettings
                .getBuilder()
                .withSettings("testing.properties")
                .build()
                .get();
        Lists.newArrayList(LOG_NAMES)
                .forEach(Unchecked.consumer(s -> this.logNamespace.deleteLog(s)));
    }

    @Test
    public void should_read_one_record() throws IOException {
        int i = 0;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final CountDownLatch barrier = new CountDownLatch(1);
        final List<String> results = Lists.newArrayList();

        final Either<Throwable, Flowable<EmissionWithOrigin>> tail = new DLTail(logManager, DLUtil.firstDlsn(logManager))
                .getTail();

        tail
                .getOrElseThrow((Supplier<IOException>) IOException::new)
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(t -> {
                    tail.get();
                })
                .doOnNext(emission -> {
                    results.add(new String(emission.getPayload().array(), UTF_8));
                    barrier.countDown();
                })
                .subscribe();

        Utils.write(logManager, ByteBuffer.wrap("--pepperoni".getBytes(UTF_8)));

        barrier.await();
        assertThat(results).containsExactly("--pepperoni");
    }

    @Test
    public void should_read_records_in_order() throws IOException {
        int i = 1;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final CountDownLatch barrier = new CountDownLatch(4);
        final List<String> results = Lists.newArrayList();

        final Either<Throwable, Flowable<EmissionWithOrigin>> tail = new DLTail(logManager, DLUtil.firstDlsn(logManager)).getTail();

        tail
                .getOrElseThrow((Supplier<IOException>) IOException::new)
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(t -> {
                    return tail.get();
                })
                .doOnNext(emission -> {
                    results.add(new String(emission.getPayload().array(), UTF_8));
                    barrier.countDown();
                })
                .subscribe();

        Utils.write(logManager, ByteBuffer.wrap("one".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("two".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("three".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("four".getBytes(UTF_8)));

        barrier.await();
        assertThat(results).containsExactly("one", "two", "three", "four");
    }

    @Test
    public void should_complete_when_reader_close() throws IOException {
        int i = 2;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final CountDownLatch barrier = new CountDownLatch(3);
        final List<String> results = Lists.newArrayList();

        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        reader.getTail()
                .getOrElseThrow((Supplier<IOException>) IOException::new)
                .subscribeOn(Schedulers.io())
                .doOnComplete(barrier::countDown)
                .doOnNext(emission -> {
                    results.add(new String(emission.getPayload().array(), UTF_8));
                    barrier.countDown();
                })
                .subscribe();

        Utils.write(logManager, ByteBuffer.wrap("one".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("two".getBytes(UTF_8)));

        reader.close();

        barrier.await();

        assertThat(results).containsExactly("one", "two");
    }


    @SuppressWarnings("unchecked")
    @Test
    public void should_recover_when_error_but_created() throws IOException {
        int i = 2;
        logNamespace.createLog(LOG_NAMES[i]);

        final DistributedLogManager logManager = Mockito.mock(DistributedLogManager.class);
        final AsyncLogReader toFail = Mockito.mock(AsyncLogReader.class, "to-fail");
        final AsyncLogReader recovered = Mockito.mock(AsyncLogReader.class, "to-recover");
        final LogRecordWithDLSN firstRecord = Mockito.mock(LogRecordWithDLSN.class);
        final LogRecordWithDLSN secondRecord = Mockito.mock(LogRecordWithDLSN.class);

        when(firstRecord.getPayload())
                .thenReturn("emitted-by-mock-1".getBytes(StandardCharsets.UTF_8));
        when(secondRecord.getPayload())
                .thenReturn("emitted-by-mock-2".getBytes(StandardCharsets.UTF_8));
        when(logManager.getLastDLSN())
                .thenReturn(DLSN.InitialDLSN);
        when(logManager.openAsyncLogReader(any(DLSN.class)))
                .thenReturn(Future.value(toFail))
                .thenReturn(Future.value(recovered));

        when(toFail.readNext())
                .thenReturn(Future.value(firstRecord))
                .thenReturn(Future.exception(new IOException("from-mock")));
        when(recovered.readNext())
                .thenReturn(Future.value(secondRecord))
                .thenAnswer(mock -> {
                    new CountDownLatch(1).await();
                    return Future.value(secondRecord);
                });

        final CountDownLatch barrier = new CountDownLatch(2);
        final List<String> results = Lists.newLinkedList();

        final DLTail reader = new DLTail(logManager, DLUtil.lastDlsn(logManager));

        reader.getTail()
                .getOrElseThrow((Supplier<IOException>) IOException::new)
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(throwable -> {
                    log.warn("recovering by creating new source");
                    return new DLTail(logManager, DLUtil.lastDlsn(logManager))
                            .getTail()
                            .getOrElseThrow((Supplier<IOException>) IOException::new);
                })
                .doOnNext(emission -> {
                    final String payload = new String(emission.getPayload().array(), StandardCharsets.UTF_8);
                    log.info("emitted: {}", payload);
                    results.add(payload);
                    barrier.countDown();
                })
                .subscribe();

        barrier.await();
        assertThat(results).containsExactly("emitted-by-mock-1", "emitted-by-mock-2");
    }


    @Test
    public void should_be_left_when_log_doesnt_exists() throws IOException {
        int i = 3;
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));
        final Throwable left = reader.getTail().getLeft();
        assertThat(left).hasMessage("initial DLSN to read is invalid in log read-hand-4");
    }


}