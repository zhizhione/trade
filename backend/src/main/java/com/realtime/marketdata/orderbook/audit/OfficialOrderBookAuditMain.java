package com.realtime.marketdata.orderbook.audit;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Reads the line protocol emitted by the DBN decoder and validates the real Java L3 engine. */
public final class OfficialOrderBookAuditMain {
    private static final int DEPTH = 10;

    private OfficialOrderBookAuditMain() {
    }

    public static void main(String[] args) throws Exception {
        MboBookEngine engine = new MboBookEngine(false, DEPTH);
        Map<MboBookEngine.BookKey, MboBookEngine.BookSnapshot> lastAppliedSnapshots = new HashMap<>();
        long mboRecords = 0;
        long lastMessages = 0;
        long tradeMessages = 0;
        long mbpMatched = 0;
        long mbpExact = 0;
        long tbboMatched = 0;
        long tbboExact = 0;
        long mismatchCount = 0;
        List<String> examples = new ArrayList<>();
        try (BufferedReader input = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8), 1 << 20
        )) {
            String line;
            while ((line = input.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] fields = line.split("\\|", -1);
                if ("M".equals(fields[0])) {
                    MboEvent event = parseEvent(fields);
                    var snapshot = engine.apply(event);
                    snapshot.ifPresent(value -> lastAppliedSnapshots.put(value.key(), value));
                    mboRecords += 1;
                    if (event.action() == 'T') tradeMessages += 1;
                    if ((event.flags() & MboBookEngine.F_LAST) != 0) lastMessages += 1;
                    continue;
                }
                if (!"R".equals(fields[0])) {
                    throw new IllegalArgumentException("unknown audit line type: " + fields[0]);
                }
                Reference reference = parseReference(fields);
                MboBookEngine.BookKey referenceKey =
                    new MboBookEngine.BookKey(reference.publisherId, reference.instrumentId);
                MboBookEngine.BookSnapshot snapshot = lastAppliedSnapshots.get(referenceKey);
                if (snapshot == null || snapshot.sequence() != reference.sequence
                    || snapshot.tsEventNs() != reference.tsEvent) {
                    snapshot = engine.snapshot(reference.publisherId, reference.instrumentId, reference.depth);
                }
                boolean exact = levels(snapshot.bids(), reference.depth).equals(reference.bids)
                    && levels(snapshot.asks(), reference.depth).equals(reference.asks);
                if ("mbp10".equals(reference.source)) {
                    mbpMatched += 1;
                    if (exact) mbpExact += 1;
                } else {
                    tbboMatched += 1;
                    if (exact) tbboExact += 1;
                }
                if (!exact) {
                    mismatchCount += 1;
                    if (examples.size() < 10) {
                        examples.add(reference.source + " mismatch key=(" + reference.sequence + ","
                            + reference.tsEvent + ") expected=" + reference.bids + "/" + reference.asks
                            + " actual=" + levels(snapshot.bids(), reference.depth) + "/"
                            + levels(snapshot.asks(), reference.depth));
                    }
                }
            }
        }
        System.out.println("Java MBO records: " + mboRecords);
        System.out.println("Java MBO F_LAST messages: " + lastMessages);
        System.out.println("Java MBO trade messages: " + tradeMessages);
        System.out.println("Java MBP-10 matched/exact: " + mbpMatched + "/" + mbpExact);
        System.out.println("Java TBBO matched/exact: " + tbboMatched + "/" + tbboExact);
        System.out.println("Java mismatch count: " + mismatchCount);
        examples.forEach(System.out::println);
        if (mismatchCount != 0) System.exit(1);
    }

    private static MboEvent parseEvent(String[] f) {
        if (f.length != 16) throw new IllegalArgumentException("MBO audit line fields=" + f.length);
        return new MboEvent(
            Long.parseUnsignedLong(f[1]), Long.parseUnsignedLong(f[2]), Long.parseUnsignedLong(f[3]),
            Integer.parseInt(f[4]), Integer.parseInt(f[5]), Long.parseLong(f[6]), f[7].charAt(0), f[8].charAt(0),
            Long.parseLong(f[9]), Long.parseLong(f[10]), Integer.parseInt(f[11]), Long.parseLong(f[12]),
            Integer.parseInt(f[13]), Integer.parseInt(f[14]), Long.parseLong(f[15])
        );
    }

    private static Reference parseReference(String[] f) {
        if (f.length != 9) throw new IllegalArgumentException("reference audit line fields=" + f.length);
        return new Reference(
            f[1], Integer.parseInt(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
            Long.parseLong(f[5]), Integer.parseInt(f[6]), decodeLevels(f[7]), decodeLevels(f[8])
        );
    }

    private static List<Level> decodeLevels(String encoded) {
        if (encoded.isEmpty()) return List.of();
        List<Level> result = new ArrayList<>();
        for (String item : encoded.split(",")) {
            String[] values = item.split(":");
            result.add(new Level(Long.parseLong(values[0]), Long.parseLong(values[1]), Integer.parseInt(values[2])));
        }
        return List.copyOf(result);
    }

    private static List<Level> levels(List<MboBookEngine.Level> source, int depth) {
        return source.stream().limit(depth)
            .map(level -> new Level(level.priceNano(), level.size(), level.orderCount())).toList();
    }

    private record Reference(
        String source, int publisherId, long instrumentId, long sequence, long tsEvent, int depth,
        List<Level> bids, List<Level> asks
    ) {
    }

    private record Level(long price, long size, int count) {
    }
}
