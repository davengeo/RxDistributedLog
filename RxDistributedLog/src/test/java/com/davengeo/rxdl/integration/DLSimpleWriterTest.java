package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.DLWithSettings;
import com.google.common.collect.Lists;
import com.davengeo.rxdl.DLSimpleWriter;
import com.davengeo.rxdl.DLWriter;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.AlreadyClosedException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import io.vavr.control.Either;
import org.jooq.lambda.Unchecked;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;


public class DLSimpleWriterTest {

    private static final String[] LOG_NAMES = {
            "s-writer-1", "s-writer-2",
            "s-writer-4", "s-writer-3"
    };

    private DistributedLogNamespace logNamespace;

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
    public void should_write_something_to_the_log() throws Exception {
        int i = 0;
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        DLWriter writer = new DLSimpleWriter(logManager);
        writer.write(ByteBuffer.wrap("writing_this_to_log".getBytes(UTF_8)));
        final DLSN lastDLSN = logManager.getLastDLSN();
        final AsyncLogReader reader = FutureUtils.result(logManager.openAsyncLogReader(lastDLSN));
        final LogRecordWithDLSN record = FutureUtils.result(reader.readNext());
        writer.close();
        assertThat(new String(record.getPayload(), UTF_8)).isEqualTo("writing_this_to_log");
    }

    @Test
    public void should_write_at_the_end_of_the_log() throws Exception {
        int i = 1;
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        DLWriter writer = new DLSimpleWriter(logManager);
        final Either<Throwable, DLSN> result = writer.write(ByteBuffer.wrap("also_this_writes_to_log".getBytes(UTF_8)));
        final DLSN lastDLSN = logManager.getLastDLSN();
        final AsyncLogReader reader = FutureUtils.result(logManager.openAsyncLogReader(lastDLSN));
        final LogRecordWithDLSN record = FutureUtils.result(reader.readNext());
        writer.close();
        assertThat(result.get()).isEqualTo(record.getDlsn());
    }

    @Test(expected = AlreadyClosedException.class)
    public void should_close_the_log_manager() throws Exception {
        int i = 2;
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        DLWriter writer = new DLSimpleWriter(logManager);
        writer.write(ByteBuffer.wrap("closing_writer".getBytes(UTF_8)));
        writer.close();

        // it should throw exception AlreadyClosedException
        logManager.getLogRecordCount();

        fail();
    }

    @Test
    public void should_close_the_writer() throws Exception {
        int i = 3;
        DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        DLWriter writer = new DLSimpleWriter(logManager);
        writer.write(ByteBuffer.wrap("closing_writer".getBytes(UTF_8)));
        writer.close();

        final Either<Throwable, DLSN> result = writer.write(ByteBuffer.wrap("shouldn't happen".getBytes(UTF_8)));
        logManager = logNamespace.openLog(LOG_NAMES[i]);
        final DLSN lastDLSN = logManager.getLastDLSN();
        final AsyncLogReader reader = FutureUtils.result(logManager.openAsyncLogReader(lastDLSN));
        final LogRecordWithDLSN record = FutureUtils.result(reader.readNext());
        assertThat(new String(record.getPayload(), UTF_8)).isEqualTo("closing_writer");
        assertThat(result.getLeft()).isInstanceOf(AlreadyClosedException.class);
    }

}