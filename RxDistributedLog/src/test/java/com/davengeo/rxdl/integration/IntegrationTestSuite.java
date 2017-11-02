package com.davengeo.rxdl.integration;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DLTailComposerTest.class,
        DLTailTest.class,
        DLUtilTest.class,
        DLWithSettingsTest.class,
        DLSimpleWriterTest.class,
        DLMultiTailTest.class
})
public class IntegrationTestSuite {
}
