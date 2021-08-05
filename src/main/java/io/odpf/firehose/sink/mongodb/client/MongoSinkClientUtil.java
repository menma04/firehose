package io.odpf.firehose.sink.mongodb.client;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MongoSinkClientUtil {

    /**
     * Gets status codes as list.
     *
     * @param mongoRetryStatusCodeBlacklist the mongo retry status code blacklist
     * @return the status codes as list
     * @since 0.1
     */
    public static List<Integer> getStatusCodesAsList(String mongoRetryStatusCodeBlacklist) {
        try {
            return Arrays
                    .stream(mongoRetryStatusCodeBlacklist.split(","))
                    .map(String::trim)
                    .filter(s -> (!s.isEmpty()))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Status code must be an integer");
        }
    }
}
