package io.odpf.firehose.sink.mongodb.client;

import io.odpf.firehose.sink.mongodb.client.MongoSinkClientUtil;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoSinkClientUtilTest {
    @Test
    public void shouldReturnBlackListRetryStatusCodesAsList() {
        String inputRetryStatusCodeBlacklist = "404, 502";
        List<Integer> statusCodesAsList = MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(404, statusCodesAsList.get(0).intValue());
        assertEquals(502, statusCodesAsList.get(1).intValue());
    }

    @Test
    public void shouldReturnEmptyBlackListRetryStatusCodesAsEmptyList() {
        String inputRetryStatusCodeBlacklist = "";
        List<Integer> statusCodesAsList = MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }
}
