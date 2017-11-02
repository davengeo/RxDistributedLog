package com.davengeo.rxdl;

import com.google.common.collect.Lists;
import io.reactivex.Flowable;
import io.vavr.control.Either;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The Simplest way to compose several readers honoring the order of emission of their records.
 * It is based in the merge operation from Reactive Extensions.
 * There are other ways to merge but they are not so simple.
 * For a well known number of readers this is the preferred way to compose them.
 */
public class DLTailComposer implements DLReader<EmissionWithOrigin> {

    private final List<DLTail> readers;
    private final Flowable<EmissionWithOrigin> compose;

    /**
     * DLTailComposer constructor.
     *
     * it creates a new multi-reader by merge the readers in the param.
     *
     * @param readers set of readers to merge. They should not have been initialised (getTail).
     */
    public DLTailComposer(@NonNull DLTail... readers) {
        this.readers = Lists.newArrayList(readers);

        //todo: pass this logic to getTail
        final List<Flowable<EmissionWithOrigin>> flowables = this.readers
                .stream()
                .map(dlTail -> dlTail.getTail().get())
                .collect(Collectors.toList());
        this.compose = Flowable.merge(flowables);
    }

    /**
     * Return the flowable of emissions of this multi-reader. This flowable honors the order of emission.
     *
     * @return either the flowable of emissions or an exception
     */
    public Either<Throwable, Flowable<EmissionWithOrigin>> getTail() {
        return Either.right(compose);
    }

    /**
     * It closes the reader so there is no more events to emit.
     * It closes all the readers composing this multi-reader.
     *
     */
    public void close() {
        for (DLTail reader : readers) {
            reader.close();
        }
    }

}
