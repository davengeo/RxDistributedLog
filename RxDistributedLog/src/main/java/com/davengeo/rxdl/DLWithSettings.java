package com.davengeo.rxdl;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URI;
import java.util.Objects;

import static com.twitter.distributedlog.DistributedLogConstants.LOCAL_REGION_ID;
import static com.twitter.distributedlog.DistributedLogConstants.LOCK_IMMEDIATE;

/**
 * A distributed log builder which accepts a properties file as parameter.
 * The property file has the specification about how to build the namespace manager.
 * Please look in the test resources folder to get some examples of these properties files.
 */
@Slf4j
public class DLWithSettings {

    private String propertiesName;
    private DistributedLogNamespace injectedManager;

    private DLWithSettings() {
    }

    /**
     * Add settings file to the builder.
     * The following setting are going to be used to build the Distributed Namespace Manager.
     *
     * @param propertiesName properties name in resources to build the manager.
     * @return the builder.
     */
    public DLWithSettings withSettings(@NonNull String propertiesName) {
        this.propertiesName = propertiesName;
        return this;
    }

    /**
     * Add an already existing Distributed Namespace Manager.
     * This should might be used for testing purposes.
     *
     * @param injectedManager the log manager to inject into the builder.
     * @return the builder.
     */
    public DLWithSettings butInjectedManager(@NonNull DistributedLogNamespace injectedManager) {
        this.injectedManager = injectedManager;
        return this;
    }

    /**
     * it builds the distributed log manager.
     *
     * @return either the distributed log manager or the first exception happened during the build.
     */
    public Either<Throwable, DistributedLogNamespace> build() {
        if (Objects.isNull(injectedManager)) {
            return fabricFromProperties();
        }
        return Either.right(injectedManager);
    }

    private Either<Throwable, DistributedLogNamespace> fabricFromProperties() {
        if (!Objects.isNull(propertiesName)) {
            return Either.left(new IllegalStateException("no properties file added in this builder"));
        }
        final Settings settings = Try
                .of(() -> new Settings(this.propertiesName))
                .getOrElseThrow(() -> new IllegalStateException(String.format("properties file %s not found", propertiesName)));
        final DistributedLogConfiguration logConfiguration = new DistributedLogConfiguration()
                .setReadAheadWaitTime(settings.readAheadWaitTime)
                .setOutputBufferSize(0)
                .setCreateStreamIfNotExists(true)
                .setImmediateFlushEnabled(true)
                .setFailFastOnStreamNotReady(true)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setLockTimeout(LOCK_IMMEDIATE);
        return Try
                .of(() -> DistributedLogNamespaceBuilder.newBuilder()
                        .clientId(settings.clientId)
                        .regionId(LOCAL_REGION_ID)
                        .conf(logConfiguration)
                        .uri(URI.create(settings.namespace))
                        .build())
                .onFailure(t -> log.error(
                        "Found error {} when building logManager from properties {}",
                        t.getMessage(), propertiesName))
                .toEither();
    }

    /**
     * It gets a new instance of this builder.
     *
     * @return the new instance of this builder.
     */
    public static DLWithSettings getBuilder() {
        return new DLWithSettings();
    }

    class Settings {

        @Getter
        private final String namespace;
        @Getter
        private final String clientId;
        @Getter
        private final int periodicFlushFrequency;
        @Getter
        private int readAheadWaitTime;

        Settings(String properties) throws ConfigurationException {
            Configuration configuration = new PropertiesConfiguration(properties);
            this.periodicFlushFrequency = configuration.getInt("dlog.periodicFlushFrequency");
            this.namespace = configuration.getString("dlog.namespace");
            this.clientId = configuration.getString("dlog.clientId");
            this.readAheadWaitTime = configuration.getInt("dlog.readAheadWaitTime");
        }

    }

}
