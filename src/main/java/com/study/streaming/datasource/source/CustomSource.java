package com.study.streaming.datasource.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class CustomSource implements Source<Long, CustomSource.RangeSplit, Collection<CustomSource.RangeSplit>>, ResultTypeQueryable<Long> {

    private final Long from;
    private final Long to;

    public CustomSource(Long from, Long to) {
        this.from = from;
        this.to = to;


    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Long, RangeSplit> createReader(SourceReaderContext sourceReaderContext) {
        return new RangeSourceReader(sourceReaderContext);
    }

    @Override
    public SplitEnumerator<RangeSplit, Collection<RangeSplit>> createEnumerator(SplitEnumeratorContext<RangeSplit> splitEnumeratorContext) throws Exception {
        int total = (int) (to - from + 1);
        Queue<RangeSplit> list = new ArrayDeque<>();
        int id = 0;
        if ((total & 1) == 0) {
            list.add(new RangeSplit(from, from + total / 2 - 1, "" + id));
            id++;
            list.add(new RangeSplit(from + total / 2, to, "" + id));
        } else {
            list.add(new RangeSplit(from, from + (total + 1) / 2 - 1, "" + id));
            id++;
            list.add(new RangeSplit(from + (total + 1) / 2 - 1, to, "" + id));

        }

        return new RangeSplitEnumerator(splitEnumeratorContext, list);
    }

    @Override
    public SplitEnumerator<RangeSplit, Collection<RangeSplit>> restoreEnumerator(SplitEnumeratorContext<RangeSplit> splitEnumeratorContext, Collection<RangeSplit> rangeSplits) {
        return new RangeSplitEnumerator(splitEnumeratorContext, rangeSplits);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        return new RangeSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<RangeSplit>> getEnumeratorCheckpointSerializer() {
        return new RangeCheckpointSerializer();
    }

    @Override
    public TypeInformation<Long> getProducedType() {
        return Types.LONG;
    }

    public static class RangeSplit implements SourceSplit {
        private String splitId;
        private Long from;
        private Long to;

        private Long current;

        public RangeSplit(Long from, Long to, String splitId) {
            this.splitId = splitId;
            this.from = from;
            this.to = to;
            this.current = from;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        public Long getFrom() {
            return from;
        }

        public Long getTo() {
            return to;
        }

        public Long getCurrent() {
            if (current <= to) {
                return current++;
            }
            return null;
        }


    }


    public static class RangeSourceReader implements SourceReader<Long, RangeSplit> {
        private final SourceReaderContext context;
        private final CompletableFuture<Void> availability;
        private RangeSplit currentSplit;
        private Queue<RangeSplit> remainingSplits;

        public RangeSourceReader(SourceReaderContext context) {
            this.context = context;
            this.availability = new CompletableFuture();

        }

        @Override
        public void start() {
            if (Objects.isNull(currentSplit)) {
                context.sendSplitRequest();
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Long> readerOutput) {
            Long current;
            if (Objects.nonNull(currentSplit) && Objects.nonNull(current = currentSplit.getCurrent())) {
                readerOutput.collect(current);
                return InputStatus.MORE_AVAILABLE;
            } else if (this.remainingSplits == null) {
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                this.currentSplit = remainingSplits.poll();
                if (this.currentSplit != null) {
                    return this.pollNext(readerOutput);
                } else {
                    return InputStatus.END_OF_INPUT;
                }
            }

        }

        @Override
        public List<RangeSplit> snapshotState(long l) {
            if (this.remainingSplits == null) {
                return Collections.emptyList();
            } else {
                ArrayList<RangeSplit> allSplits = new ArrayList(1 + this.remainingSplits.size());
                if (Objects.nonNull(this.currentSplit)) {
                    allSplits.add(currentSplit);
                }
                allSplits.addAll(this.remainingSplits);
                return allSplits;
            }
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availability;
        }

        @Override
        public void addSplits(List<RangeSplit> list) {
            this.remainingSplits = new ArrayDeque(list);
            this.availability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            this.remainingSplits = new ArrayDeque();
        }

        @Override
        public void close() {

        }
    }


    public static class RangeSplitEnumerator implements SplitEnumerator<RangeSplit, Collection<RangeSplit>> {
        private final SplitEnumeratorContext<RangeSplit> context;
        private final Queue<RangeSplit> remainingSplits;

        public RangeSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, Collection<RangeSplit> remainingSplits) {
            this.context = context;
            this.remainingSplits = new ArrayDeque<>(remainingSplits);

        }

        @Override
        public void start() {

        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String s) {
            RangeSplit nextSplit = this.remainingSplits.poll();
            if (nextSplit != null) {
                this.context.assignSplit(nextSplit, subtaskId);
            } else {
                this.context.signalNoMoreSplits(subtaskId);
            }

        }

        @Override
        public void addSplitsBack(List<RangeSplit> list, int i) {
            this.remainingSplits.addAll(list);
        }

        @Override
        public void addReader(int i) {

        }

        @Override
        public Collection<RangeSplit> snapshotState() {
            return this.remainingSplits;
        }

        @Override
        public void close() {

        }
    }


    public static class RangeSplitSerializer implements SimpleVersionedSerializer<RangeSplit> {


        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(RangeSplit rangeSplit) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(rangeSplit.splitId().length() + 18);
            serializeV1(out, rangeSplit);
            return out.getCopyOfBuffer();
        }

        static void serializeV1(DataOutputView out, RangeSplit split) throws IOException {
            out.writeUTF(split.splitId());
            out.writeLong(split.getFrom());
            out.writeLong(split.getTo());
        }

        public static RangeSplit deserializeV1(DataInputView in) throws IOException {
            String splitId = in.readUTF();
            return new RangeSplit(in.readLong(), in.readLong(), splitId);
        }

        @Override
        public RangeSplit deserialize(int version, byte[] bytes) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(bytes);
            return deserializeV1(in);
        }
    }

    public static class RangeCheckpointSerializer implements SimpleVersionedSerializer<Collection<RangeSplit>> {


        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Collection<RangeSplit> checkpoint) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(checkpoint.size() * 22 + 4);
            out.writeInt(checkpoint.size());
            Iterator var3 = checkpoint.iterator();

            while (var3.hasNext()) {
                RangeSplit split = (RangeSplit) var3.next();
                RangeSplitSerializer.serializeV1(out, split);
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public Collection<RangeSplit> deserialize(int i, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            int num = in.readInt();
            ArrayList<RangeSplit> result = new ArrayList(num);

            for (int remaining = num; remaining > 0; --remaining) {
                result.add(RangeSplitSerializer.deserializeV1(in));
            }
            return result;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> dataStream = env.fromSource(new CustomSource(1L, 100L), WatermarkStrategy.noWatermarks(), "source");
        dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("*");

        dataStream.print();
        env.execute("CustomSource");
    }
}
