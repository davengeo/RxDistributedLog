package com.davengeo.rxdl;

import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.util.Future;
import io.vavr.control.Either;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class DLUtilRobustnessTest {

    @Test
    public void firstDlsn_should_make_invalid_when_null_answers_from_manager() {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        final DLSN dlsn = DLUtil.firstDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InvalidDLSN);
    }

    @Test
    public void firstDlsn_should_make_invalid_when_future_returns_exception() {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        when(logManager.getFirstDLSNAsync()).thenReturn(Future.exception(new IOException("from async call")));
        final DLSN dlsn = DLUtil.firstDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InvalidDLSN);
    }

    @Test
    public void firstDlsn_should_return_a_mocked_dlsn() {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        DLSN mocked = mock(DLSN.class);
        when(logManager.getFirstDLSNAsync()).thenReturn(Future.value(mocked));
        final DLSN dlsn = DLUtil.firstDlsn(logManager);
        assertThat(dlsn).isEqualTo(mocked);
    }

    @Test
    public void firstDlsn_should_raise_when_null_manager() {
        try {
            DLUtil.firstDlsn(null);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("logManager is null");
        }

    }

    @Test
    public void lastDlsn_should_raise_when_null_manager() {
        try {
            DLUtil.lastDlsn(null);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("logManager is null");
        }
    }

    @Test
    public void lastDlsn_should_return_invalid_when_io_exception() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        when(logManager.getLastDLSN()).thenThrow(new IOException("from manager"));
        final DLSN dlsn = DLUtil.lastDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InvalidDLSN);
    }

    @Test
    public void lastDlsn_should_return_mocked_when_called() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        DLSN mocked = mock(DLSN.class);
        when(logManager.getLastDLSN()).thenReturn(mocked);
        final DLSN dlsn = DLUtil.lastDlsn(logManager);
        assertThat(dlsn).isEqualTo(mocked);
    }

    @Test
    public void write_should_be_left_when_getAppendOnlyStreamWriter_fails() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        when(logManager.getAppendOnlyStreamWriter()).thenThrow(new IOException("from manager"));

        final Either<Throwable, DLSN> result = Utils.write(logManager, ByteBuffer.wrap("success".getBytes()));
        assertThat(result.getLeft()).hasMessage("from manager");
    }

    @Test
    public void write_should_be_left_when_getAppendOnlyStreamWriter_fails_in_write() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);

        when(logManager.getAppendOnlyStreamWriter()).thenReturn(writer);
        when(writer.write(any())).thenThrow(new RuntimeException("from writer"));

        final Either<Throwable, DLSN> result = Utils.write(logManager, ByteBuffer.wrap("success".getBytes()));
        assertThat(result.getLeft()).hasMessage("from writer");
    }

    @Test
    public void write_should_be_left_when_getAppendOnlyStreamWriter_fails_in_write_but_closes() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);

        when(logManager.getAppendOnlyStreamWriter()).thenReturn(writer);
        when(writer.write(any())).thenThrow(new RuntimeException("from writer 2"));
        doNothing().when(writer).close();

        final Either<Throwable, DLSN> result = Utils.write(logManager, ByteBuffer.wrap("success".getBytes()));
        assertThat(result.getLeft()).hasMessage("from writer 2");
        verify(writer, times(1)).close();
    }

    @Test
    public void write_should_be_right_when_getAppendOnlyStreamWriter_gets_DLSN() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        DLSN mock = mock(DLSN.class);

        when(logManager.getAppendOnlyStreamWriter()).thenReturn(writer);
        when(writer.write(any())).thenReturn(Future.value(mock));

        final Either<Throwable, DLSN> result = Utils.write(logManager, ByteBuffer.wrap("success".getBytes()));
        assertThat(result.get()).isEqualTo(mock);
    }

    @Test
    public void write_should_be_left_when_getAppendOnlyStreamWriter_fails_to_close() throws IOException {
        DistributedLogManager logManager = mock(DistributedLogManager.class);
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        DLSN mock = mock(DLSN.class);

        when(logManager.getAppendOnlyStreamWriter()).thenReturn(writer);
        doThrow(new IOException("when close")).when(writer).close();
        when(writer.write(any())).thenReturn(Future.value(mock));

        final Either<Throwable, DLSN> result = Utils.write(logManager, ByteBuffer.wrap("success".getBytes()));
        assertThat(result.getLeft()).hasMessage("when close");
    }

}