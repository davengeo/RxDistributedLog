package com.davengeo.rxdl;

import com.twitter.distributedlog.DLSN;
import io.vavr.control.Either;

import java.nio.ByteBuffer;

/**
 * Writer interface for distributed log. The interface express an auto closeable resource
 * which is able to write in a given distributed log.
 *
 */
public interface DLWriter extends AutoCloseable {
    /**
     * It tries to write in the distributed log with a payload using the bytebuffer content.
     *
     * @param content the content expressed as bytebuffer.
     * @return either the next position of the log after writing or an exception.
     */
    Either<Throwable, DLSN> write(ByteBuffer content);
}
