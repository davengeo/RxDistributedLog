package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.*;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.util.CountDownLatch;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.jooq.lambda.Unchecked;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DLMultiTailTest {

    private DistributedLogNamespace logNamespace;

    private static final String[] LOG_NAMES = {
            "multi-hand-1", "multi-hand-2",
            "multi-hand-3", "multi-hand-4",
            "multi-hand-5", "multi-hand-6",
            "to-close-0",
            "multi-hand-7",
            "multi-hand-8", "multi-hand-9",
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
    public void should_receive_emission_from_original_source() throws Exception {

        int i = 0;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);

        final CountDownLatch barrier = new CountDownLatch(2);
        final DLMultiTail multiTail = new DLMultiTail(logManager, DLUtil.lastDlsn(logManager));
        final List<EmissionWithOrigin> results = Lists.newArrayList();

        givenSubscription(barrier, multiTail, results);

        Utils.write(logManager, ByteBuffer.wrap("uno".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("dos".getBytes(UTF_8)));

        barrier.await();
        final List<String> emissions = results.stream()
                .map(emissionWithOrigin -> new String(emissionWithOrigin.getPayload().array(), UTF_8))
                .collect(Collectors.toList());
        assertThat(emissions).containsExactly("uno", "dos");
    }


    @Test
    public void should_receive_emission_from_one_added_source() throws Exception {

        logNamespace.createLog(LOG_NAMES[1]);
        logNamespace.createLog(LOG_NAMES[2]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[1]);
        final DistributedLogManager logManager2 = logNamespace.openLog(LOG_NAMES[2]);

        final CountDownLatch barrier = new CountDownLatch(3);
        final DLMultiTail multiTail = new DLMultiTail(logManager, DLUtil.lastDlsn(logManager));
        final List<EmissionWithOrigin> results = Lists.newArrayList();

        givenSubscription(barrier, multiTail, results);

        Utils.write(logManager, ByteBuffer.wrap("one".getBytes(UTF_8)));
        Utils.write(logManager, ByteBuffer.wrap("two".getBytes(UTF_8)));

        multiTail.addSources(logManager2, DLUtil.lastDlsn(logManager2));

        Utils.write(logManager2, ByteBuffer.wrap("three".getBytes(UTF_8)));
        Utils.write(logManager2, ByteBuffer.wrap("four".getBytes(UTF_8)));

        barrier.await();
        final List<String> emissions = results.stream()
                .map(emissionWithOrigin -> new String(emissionWithOrigin.getPayload().array(), UTF_8))
                .collect(Collectors.toList());
        assertThat(emissions).containsExactly("one", "two", "three", "four");
    }

    @Test
    public void should_receive_emission_from_two_added_sources() throws Exception {

        logNamespace.createLog(LOG_NAMES[3]);
        logNamespace.createLog(LOG_NAMES[4]);
        logNamespace.createLog(LOG_NAMES[5]);
        final DistributedLogManager logManager3 = logNamespace.openLog(LOG_NAMES[3]);
        final DistributedLogManager logManager4 = logNamespace.openLog(LOG_NAMES[4]);
        final DistributedLogManager logManager5 = logNamespace.openLog(LOG_NAMES[5]);

        final CountDownLatch barrier = new CountDownLatch(4);
        final DLMultiTail multiTail = new DLMultiTail(logManager3, DLUtil.lastDlsn(logManager3));
        final List<EmissionWithOrigin> results = Lists.newArrayList();

        givenSubscription(barrier, multiTail, results);

        Utils.write(logManager3, ByteBuffer.wrap("one".getBytes(UTF_8)));
        Utils.write(logManager3, ByteBuffer.wrap("two".getBytes(UTF_8)));

        multiTail.addSources(logManager4, DLUtil.lastDlsn(logManager4));
        multiTail.addSources(logManager5, DLUtil.lastDlsn(logManager5));

        Utils.write(logManager4, ByteBuffer.wrap("three".getBytes(UTF_8)));
        Utils.write(logManager5, ByteBuffer.wrap("four".getBytes(UTF_8)));

        barrier.await();
        final List<String> emissions = results.stream()
                .map(emissionWithOrigin -> new String(emissionWithOrigin.getPayload().array(), UTF_8))
                .collect(Collectors.toList());
        assertThat(emissions).containsExactly("one", "two", "three", "four");
    }


    @Test
    public void should_close_and_emit_complete() throws Exception {
        int i = 6;
        logNamespace.createLog(LOG_NAMES[i]);
        final DistributedLogManager logManager = logNamespace.openLog(LOG_NAMES[i]);

        final CountDownLatch barrier = new CountDownLatch(1);
        final DLMultiTail multiTail = new DLMultiTail(logManager, DLUtil.lastDlsn(logManager));

        multiTail
                .getTail()
                .get()
                .subscribeOn(Schedulers.io())
                .doOnComplete(barrier::countDown)
                .subscribe();

        multiTail.close();
        barrier.await();
    }

    @Test
    public void should_delete_source_and_stop_emitting_from_that_source() throws Exception {

        logNamespace.createLog(LOG_NAMES[7]);
        logNamespace.createLog(LOG_NAMES[8]);
        logNamespace.createLog(LOG_NAMES[9]);
        final DistributedLogManager logManager7 = logNamespace.openLog(LOG_NAMES[7]);
        final DistributedLogManager logManager8 = logNamespace.openLog(LOG_NAMES[8]);
        final DistributedLogManager logManager9 = logNamespace.openLog(LOG_NAMES[9]);

        final CountDownLatch barrier = new CountDownLatch(5);
        final DLMultiTail multiTail = new DLMultiTail(logManager7, DLUtil.lastDlsn(logManager7));
        final List<EmissionWithOrigin> results = Lists.newArrayList();

        givenSubscription(barrier, multiTail, results);

        Utils.write(logManager7, ByteBuffer.wrap("one".getBytes(UTF_8)));
        Utils.write(logManager7, ByteBuffer.wrap("two".getBytes(UTF_8)));

        multiTail.addSources(logManager8, DLUtil.lastDlsn(logManager8));
        Utils.write(logManager8, ByteBuffer.wrap("three".getBytes(UTF_8)));

        multiTail
                .addSources(logManager9, DLUtil.lastDlsn(logManager9))
                .ifPresent(throwable -> log.error("found: {}", throwable));

        Utils.write(logManager9, ByteBuffer.wrap("four".getBytes(UTF_8)));

        multiTail.deleteSources(LOG_NAMES[9]);
        Utils.write(logManager9, ByteBuffer.wrap("five".getBytes(UTF_8)));

        Utils.write(logManager8, ByteBuffer.wrap("six".getBytes(UTF_8)));

        barrier.await();
        final List<String> emissions = results.stream()
                .map(emissionWithOrigin -> new String(emissionWithOrigin.getPayload().array(), UTF_8))
                .collect(Collectors.toList());
        assertThat(emissions).containsExactly("one", "two", "three", "four", "six");
        final List<String> origins = results.stream()
                .map(EmissionWithOrigin::getOrigin)
                .collect(Collectors.toList());
        assertThat(origins).containsExactly(LOG_NAMES[7], LOG_NAMES[7], LOG_NAMES[8], LOG_NAMES[9], LOG_NAMES[8]);
    }

    private void givenSubscription(CountDownLatch barrier, DLMultiTail multiTail, List<EmissionWithOrigin> results) {
        multiTail
                .getTail()
                .getOrElseThrow((Function<Throwable, RuntimeException>) RuntimeException::new)
                .subscribeOn(Schedulers.io())
                .doOnNext(emission -> {
                    final String value = new String(emission.getPayload().array(), UTF_8);
                    results.add(emission);
                    log.info("received: {}", value);
                    barrier.countDown();
                })
                .subscribe();
    }
}

