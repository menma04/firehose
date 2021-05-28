package io.odpf.firehose.sink.file;

import io.odpf.firehose.sink.file.writer.LocalFileWriter;
import io.odpf.firehose.sink.file.writer.policy.SizeBasedRotatingPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SizeBasedRotatingPolicyTest {

    @Mock
    private LocalFileWriter fileWriter;
    private SizeBasedRotatingPolicy sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(256);

    @Test
    public void shouldNeedRotateWhenWriterDataSizeGreaterThanEqualToMaxFileSize() {
        long dataSize = 258L;
        when(fileWriter.currentSize()).thenReturn(dataSize);

        boolean shouldRotate = sizeBasedRotatingPolicy.shouldRotate(fileWriter);

        assertTrue(shouldRotate);
    }

    @Test
    public void shouldNotNeedRotateWhenSizeBelowTheLimit() {
        long dataSize = 100L;
        when(fileWriter.currentSize()).thenReturn(dataSize);

        boolean shouldRotate = sizeBasedRotatingPolicy.shouldRotate(fileWriter);

        assertFalse(shouldRotate);
    }
}