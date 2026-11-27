/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.mongo;

// tag::populate-token-masks[]
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PopulateTokenMasks {

    public static void main(String[] args) {
        // args[0] is the connection string, args[1] the database name
        try (MongoClient client = MongoClients.create(args[0])) {
            MongoCollection<Document> tokens = client.getDatabase(args[1]).getCollection("trackingtokens");

            Map<String, List<Integer>> segmentsByProcessor = new LinkedHashMap<>();
            for (Document token : tokens.find(Filters.exists("mask", false))) {
                segmentsByProcessor.computeIfAbsent(token.getString("processorName"), name -> new ArrayList<>())
                                   .add(token.getInteger("segment"));
            }

            segmentsByProcessor.forEach((processorName, segmentIds) -> {
                for (SegmentInfo segment : computeSegments(segmentIds)) {
                    tokens.updateOne(Filters.and(Filters.eq("processorName", processorName),
                                                 Filters.eq("segment", segment.segmentId())),
                                     Updates.set("mask", segment.mask()));
                }
            });
        }
    }

    private static List<SegmentInfo> computeSegments(List<Integer> segmentIds) {
        Set<SegmentInfo> result = new HashSet<>();
        computeSegments(new SegmentInfo(0, 0), segmentIds, result);
        return result.stream().sorted(Comparator.comparingInt(SegmentInfo::segmentId)).toList();
    }

    private static void computeSegments(SegmentInfo current, List<Integer> segmentIds, Set<SegmentInfo> result) {
        int newMask = (current.mask() << 1) | 1;
        int sibling = current.segmentId() + (current.mask() == 0 ? 1 : newMask ^ current.mask());
        if (segmentIds.contains(sibling)) {
            computeSegments(new SegmentInfo(current.segmentId(), newMask), segmentIds, result);
            computeSegments(new SegmentInfo(sibling, newMask), segmentIds, result);
        } else {
            result.add(current);
        }
    }

    private record SegmentInfo(int segmentId, int mask) {

    }
}
// end::populate-token-masks[]
