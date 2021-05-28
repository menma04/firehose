package io.odpf.firehose.sink.file;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.file.message.MessageSerializer;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.LocalFileWriter;
import io.odpf.firehose.sink.file.writer.PartitioningWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FileSink extends AbstractSink {

    private PartitioningWriter partitioningWriter;
    private Path basePath;
    private List<Record> records;
    private MessageSerializer serializer;

    public FileSink(Instrumentation instrumentation, String sinkType) {
        super(instrumentation, sinkType);
    }

    public FileSink(Instrumentation instrumentation, String sinkType, PartitioningWriter partitioningWriter, MessageSerializer serializer, Path basePath) {
        super(instrumentation, sinkType);
        this.serializer = serializer;
        this.basePath = basePath;
        this.partitioningWriter = partitioningWriter;
    }

    @Override
    protected List<Message> execute() throws Exception {
        for (Record record : this.records) {
            this.partitioningWriter.getWriter(basePath, record).write(record);
        }
        return new LinkedList<>();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        records = new LinkedList<>();
        for (Message message : messages) {
            Record record = serializer.serialize(message);
            records.add(record);
        }
    }

    @Override
    public void close() throws IOException {
        partitioningWriter.close();
    }
}
