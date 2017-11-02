package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.DLUtil;
import com.davengeo.rxdl.DLWithSettings;
import com.google.common.collect.Lists;
import com.davengeo.rxdl.Utils;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import io.vavr.control.Either;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DLUtilTest {

    private DistributedLogNamespace logNamespace;

    private static final String[] LOG_NAMES = {
            "free-hand-1", "free-hand-2",
            "free-hand-3", "free-hand-4",
            "test-first-1", "test-first-2"
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

    @After
    public void destroy() {
        Lists.newArrayList(LOG_NAMES)
                .forEach(Unchecked.consumer(s -> this.logNamespace.openLog(s).close()));
    }

    @Test
    public void lastDlsn_should_fetch_invalid_when_log_not_found() throws Throwable {
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[0]);
        final DLSN dlsn = DLUtil.lastDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InvalidDLSN);
    }

    @Test
    public void lastDlsn_should_fetch_initial_when_log_is_empty() throws Throwable {
        final int i = 1;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final DLSN dlsn = DLUtil.lastDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InitialDLSN);
    }


    @Test
    public void lastDlsn_should_fetch_final_when_log_is_not_empty() throws Throwable {
        final int i = 2;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        Utils.write(logManager, ByteBuffer.wrap("peperoni".getBytes(UTF_8)));
        final Either<Throwable, DLSN> lastWrittenDlsn = Utils
                .write(logManager, ByteBuffer.wrap("mantecados".getBytes(UTF_8)));
        final DLSN lastDlsn = DLUtil.lastDlsn(logManager);
        assertThat(lastDlsn).isEqualTo(lastWrittenDlsn.get());
    }

    @Test
    public void firstDlsn_should_fetch_invalid_when_log_not_found() throws Throwable {
        final int i = 3;
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final DLSN dlsn = DLUtil.firstDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InvalidDLSN);
    }

    @Test
    public void firstDlsn_should_fetch_initial_when_log_is_empty() throws Throwable {
        final int i = 4;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        final DLSN dlsn = DLUtil.firstDlsn(logManager);
        assertThat(dlsn).isEqualTo(DLSN.InitialDLSN);
    }

    @Test
    public void firstDlsn_should_fetch_first_even_when_log_is_not_empty() throws Throwable {
        final int i = 5;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);
        Utils.write(logManager, ByteBuffer.wrap("peperoni".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("mantecados".getBytes(UTF_8)));
        final DLSN firstDlsn = DLUtil.firstDlsn(logManager);
        assertThat(firstDlsn).isEqualTo(DLSN.InitialDLSN);
    }


}