package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.*;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.util.CountDownLatch;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DLTailComposerTest {

    private static final String[] LOG_NAMES = {
            "compose-hand-1", "compose-hand-2",
            "compose-hand-3", "compose-hand-4"
    };
    private DistributedLogNamespace logNamespace;

    @Before
    public void setUp() throws Exception {
        logNamespace = DLWithSettings
                .getBuilder()
                .withSettings("testing.properties")
                .build()
                .get();
        Lists.newArrayList(LOG_NAMES)
                .forEach(Unchecked.consumer(logNamespace::deleteLog));
    }

    @Test
    public void should_read_records_from_both_sources_and_emit_them_in_order() throws IOException {
        logNamespace.createLog(LOG_NAMES[0]);
        logNamespace.createLog(LOG_NAMES[1]);
        final DistributedLogManager logManagerFirst = logNamespace.openLog(LOG_NAMES[0]);
        final DistributedLogManager logManagerSecond = logNamespace.openLog(LOG_NAMES[1]);
        final CountDownLatch barrier = new CountDownLatch(2);
        final List<String> results = Lists.newArrayList();

        final DLTail first = new DLTail(logManagerFirst, DLUtil.firstDlsn(logManagerFirst));
        final DLTail second = new DLTail(logManagerSecond, DLUtil.firstDlsn(logManagerFirst));

        DLTailComposer compose = new DLTailComposer(first, second);

        compose
                .getTail()
                .get()
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(t -> {
                    return compose.getTail().get();
                })
                .doOnNext(emission -> {
                    final String payload = new String(emission.getPayload().array(), UTF_8);
                    log.info("p00r {}", payload);
                    results.add(payload);
                    barrier.countDown();
                })
                .subscribe();

        Utils.write(logManagerFirst, ByteBuffer.wrap("--pepperoni--1".getBytes(UTF_8)));
        Utils.write(logManagerSecond, ByteBuffer.wrap("--pepperoni--2".getBytes(UTF_8)));

        barrier.await();

        assertThat(results).containsExactly("--pepperoni--1", "--pepperoni--2");
    }

    @Test
    public void should_complete_when_compose_closes() throws IOException {
        logNamespace.createLog(LOG_NAMES[2]);
        logNamespace.createLog(LOG_NAMES[3]);
        final DistributedLogManager logManagerFirst = logNamespace.openLog(LOG_NAMES[2]);
        final DistributedLogManager logManagerSecond = logNamespace.openLog(LOG_NAMES[3]);
        final CountDownLatch barrier = new CountDownLatch(1);

        final DLTail first = new DLTail(logManagerFirst, DLUtil.firstDlsn(logManagerFirst));
        final DLTail second = new DLTail(logManagerSecond, DLUtil.firstDlsn(logManagerFirst));

        DLTailComposer compose = new DLTailComposer(first, second);
        AtomicBoolean flag = new AtomicBoolean(false);
        compose
                .getTail()
                .get()
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(t -> {
                    return compose.getTail().get();
                })
                .doOnComplete(() -> {
                    flag.set(true);
                    barrier.countDown();
                })
                .subscribe();

        compose.close();

        barrier.await();
        assertThat(flag.get()).isTrue();
    }


}