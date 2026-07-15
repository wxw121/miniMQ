package com.minimq.storage;

import com.minimq.common.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentCompactorTest {

    @Test
    void plansLatestKeyRecordsAndKeepsNullKeys(@TempDir Path dir) throws Exception {
        SegmentedLogConfig config = new SegmentedLogConfig(1, 1);
        SegmentFile old = segment(dir, 0, new Record(0, "a".getBytes(), "old".getBytes()), config);
        SegmentFile nullKey = segment(dir, 1, new Record(1, null, "event".getBytes()), config);
        SegmentFile active = segment(dir, 2, new Record(2, "a".getBytes(), "new".getBytes()), config);

        try (old; nullKey; active) {
            List<SegmentCompactor.SegmentReplacement> replacements =
                    SegmentCompactor.plan(List.of(old, nullKey, active), CompactionPolicy.enabled(2));

            assertEquals(2, replacements.size());
            assertEquals(0, replacements.get(0).baseOffset());
            assertTrue(replacements.get(0).retainedRecords().isEmpty());
            assertEquals(List.of(1L), replacements.get(1).retainedRecords().stream().map(Record::offset).toList());
        }
    }

    private SegmentFile segment(Path dir, long baseOffset, Record record, SegmentedLogConfig config) throws Exception {
        SegmentDescriptor descriptor = new SegmentDescriptor(baseOffset, baseOffset);
        try (SegmentFile created = SegmentFile.create(dir, descriptor, config)) {
            created.append(record.offset(), record.key(), record.value());
        }
        return SegmentFile.open(dir, descriptor, false, config);
    }
}
