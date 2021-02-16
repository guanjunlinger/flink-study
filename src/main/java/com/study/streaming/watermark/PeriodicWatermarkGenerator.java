package com.study.streaming.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class PeriodicWatermarkGenerator implements WatermarkGenerator<MyEvent> {
    private long maxTimestamp;
    private long delay;

    public PeriodicWatermarkGenerator(Long delay) {
        this.delay = delay;

    }

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput watermarkOutput) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay));
    }
}
