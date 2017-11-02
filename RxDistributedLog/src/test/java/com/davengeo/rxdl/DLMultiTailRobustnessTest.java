package com.davengeo.rxdl;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.util.CountDownLatch;
import com.twitter.util.Future;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DLMultiTailRobustnessTest {

    @Test
    public void should_be_left_when_manager_fails_to_get_reader() throws IOException {
        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doThrow(new RuntimeException("from log manager"))
                .when(logManager).getAsyncLogReader(any(DLSN.class));

        final DLMultiTail multiTail = new DLMultiTail(logManager, DLUtil.firstDlsn(logManager));

        assertThat(multiTail.getTail().getLeft())
                .hasMessage("from log manager");
    }

    @Test
    public void should_be_left_when_get_stream_name_is_exception() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doThrow(new RuntimeException("from get stream name"))
                .when(logManager).getStreamName();

        final DLMultiTail reader = new DLMultiTail(logManager, DLUtil.firstDlsn(logManager));

        assertThat(reader.getTail().getLeft())
                .hasMessage("from get stream name");
    }

    @Test
    public void should_send_error_after_successful_emission_() throws IOException {

        final DistributedLogManager logManager = mock(DistributedLogManager.class);
        final AsyncLogReader asyncLogReader = mock(AsyncLogReader.class);
        final LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);

        doReturn(Future.value(DLSN.InitialDLSN))
                .when(logManager).getFirstDLSNAsync();
        doReturn("from this test")
                .when(logManager).getStreamName();
        doReturn(asyncLogReader)
                .when(logManager).getAsyncLogReader(any(DLSN.class));
        doReturn(Future.value(record))
                .when(asyncLogReader).readNext();
        when(record.getPayload())
                .thenReturn("success payload 1".getBytes(UTF_8))
                .thenThrow(new RuntimeException("from record payload 2"));

        final DLMultiTail reader = new DLMultiTail(logManager, DLUtil.firstDlsn(logManager));
        final CountDownLatch barrier = new CountDownLatch(1);
        final List<Throwable> errors = Lists.newArrayList();
        final List<String> successes = Lists.newArrayList();

        reader
                .getTail()
                .getOrElseThrow((Function<Throwable, RuntimeException>) RuntimeException::new)
                .doOnNext(emission -> successes.add(new String(emission.getPayload().array(), UTF_8)))
                .doOnError(throwable -> {
                    errors.add(throwable);
                    barrier.countDown();
                })
                .onErrorResumeNext((Throwable t) -> reader.getTail().get())
                .subscribe();

        barrier.await();

        assertThat(errors.get(0)).hasMessage("from record payload 2");
        assertThat(errors).hasSize(1);
        assertThat(successes.get(0)).isEqualTo("success payload 1");
        assertThat(successes).hasSize(1);
    }

    @Test
    public void should_send_after_successful_emission_() throws IOException {

    }
}
