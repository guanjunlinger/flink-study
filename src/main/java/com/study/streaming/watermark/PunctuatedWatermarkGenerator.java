package com.study.streaming.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class PunctuatedWatermarkGenerator implements WatermarkGenerator<MyEvent> {

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput watermarkOutput) {
        if (event.getSpecial()) {
            watermarkOutput.emitWatermark(new Watermark(event.getTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
    }
}
