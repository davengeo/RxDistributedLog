package com.davengeo.rxdl;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.util.CountDownLatch;
import com.twitter.util.Future;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@Slf4j
public class DLTailRobustnessTest {


    @Test
    public void should_be_left_when_manager_fails_to_get_reader() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();

        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        assertThat(reader.getTail().getLeft())
                .hasMessage("Encountered exception on waiting result");
    }

    @Test
    public void should_be_left_when_reader_future_is_exception() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn(Future.exception(new IOException("from reader future")))
                .when(logManager).openAsyncLogReader(any(DLSN.class));

        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        assertThat(reader.getTail().getLeft())
                .hasMessage("from reader future");
    }

    @Test
    public void should_emit_error_when_reader_read_next_fails() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        final AsyncLogReader asyncLogReader = mock(AsyncLogReader.class);

        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn(Future.value(asyncLogReader))
                .when(logManager).openAsyncLogReader(any(DLSN.class));
        doThrow(new RuntimeException("from reader"))
                .when(asyncLogReader).readNext();

        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        CountDownLatch barrier = new CountDownLatch(1);
        List<Throwable> result = Lists.newArrayList();
        reader.getTail().get()
                .doOnError(throwable -> {
                    result.add(throwable);
                    barrier.countDown();
                })
                .subscribe();
        barrier.await();
        assertThat(result.get(0)).hasMessage("from reader");
        assertThat(result).hasSize(1);
    }

    @Test
    public void should_be_emit_error_when_record_future_is_exception() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        final AsyncLogReader asyncLogReader = mock(AsyncLogReader.class);

        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn(Future.value(asyncLogReader))
                .when(logManager).openAsyncLogReader(any(DLSN.class));
        doReturn(Future.exception(new RuntimeException("from record future")))
                .when(asyncLogReader).readNext();

        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        CountDownLatch barrier = new CountDownLatch(1);
        List<Throwable> result = Lists.newArrayList();
        reader.getTail().get()
                .doOnError(throwable -> {
                    result.add(throwable);
                    barrier.countDown();
                })
                .subscribe();
        barrier.await();
        assertThat(result.get(0)).hasMessage("from record future");
        assertThat(result).hasSize(1);
    }

    @Test
    public void should_be_emit_error_when_record_read_cannot_be_opened() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        final AsyncLogReader asyncLogReader = mock(AsyncLogReader.class);
        final LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);

        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn(Future.value(asyncLogReader))
                .when(logManager).openAsyncLogReader(any(DLSN.class));
        doReturn(Future.value(record))
                .when(asyncLogReader).readNext();
        when(record.getPayload())
                .thenThrow(new RuntimeException("from record payload"));


        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        CountDownLatch barrier = new CountDownLatch(1);
        List<Throwable> result = Lists.newArrayList();
        reader.getTail().get()
                .doOnError(throwable -> {
                    result.add(throwable);
                    barrier.countDown();
                })
                .subscribe();
        barrier.await();

        assertThat(result.get(0)).hasMessage("from record payload");
        assertThat(result).hasSize(1);
    }

    @Test
    public void should_send_error_after_successful_emission() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        final AsyncLogReader asyncLogReader = mock(AsyncLogReader.class);
        final LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);

        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn(Future.value(asyncLogReader))
                .when(logManager).openAsyncLogReader(any(DLSN.class));
        doReturn(Future.value(record))
                .when(asyncLogReader).readNext();
        when(record.getPayload())
                .thenReturn("success".getBytes(UTF_8))
                .thenThrow(new RuntimeException("from record payload 2"));


        final DLTail reader = new DLTail(logManager, DLUtil.firstDlsn(logManager));

        CountDownLatch barrier = new CountDownLatch(1);
        List<Throwable> errors = Lists.newArrayList();
        List<String> successes = Lists.newArrayList();
        reader.getTail().get()
                .doOnNext(emission -> successes.add(new String(emission.getPayload().array(), UTF_8)))
                .doOnError(throwable -> {
                    errors.add(throwable);
                    barrier.countDown();
                })
                .subscribe();
        barrier.await();

        assertThat(errors.get(0)).hasMessage("from record payload 2");
        assertThat(errors).hasSize(1);
        assertThat(successes.get(0)).isEqualTo("success");
        assertThat(successes).hasSize(1);
    }

}