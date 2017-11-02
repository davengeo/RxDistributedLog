package com.davengeo.rxdl.integration;

import com.davengeo.rxdl.DLWithSettings;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import io.vavr.control.Either;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Slf4j
public class DLWithSettingsTest {

    @Test
    public void should_inject_manager() throws IOException {
        final DistributedLogConfiguration logConfiguration = new DistributedLogConfiguration();
        DistributedLogNamespace manager = DistributedLogNamespaceBuilder
                .newBuilder()
                .uri(URI.create("distributedlog://34.230.250.177:2181/messaging/distributedlog/testing"))
                .conf(logConfiguration)
                .build();
        final DistributedLogNamespace namespace = DLWithSettings.getBuilder()
                .butInjectedManager(manager)
                .build()
                .get();
        assertThat(manager).isEqualTo(namespace);
    }

    @Test
    public void should_build_logManager() throws IOException {
        final DistributedLogNamespace logNamespace = DLWithSettings
                .getBuilder()
                .withSettings("testing.properties")
                .build()
                .get();
        final Iterator<String> logs = logNamespace.getLogs();
        logs.forEachRemaining(s -> log.info("log: {}", s));
    }

    @Test
    public void should_not_build_logManager_with_non_existing_namespace() throws IOException {
        final Either<Throwable, DistributedLogNamespace> either = DLWithSettings
                .getBuilder()
                .withSettings("bad-testing.properties")
                .build();
        assertThat(either.getLeft()).isInstanceOf(IOException.class);
    }

    @Test(expected = IllegalStateException.class)
    public void should_raise_illegal_state_when_no_properties() throws IOException {
        DLWithSettings.getBuilder().build();
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void should_raise_exception_when_properties_not_found() throws IOException {
        DLWithSettings.getBuilder().withSettings("no-existent.properties").build();
        fail();
    }

}