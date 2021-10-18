package cn.edu.buaa.network.nexmark.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;

import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.slf4j.Logger;

public class DummyLatencyCountingSink<T> extends StreamSink<T> {

	private final Logger logger;

	public DummyLatencyCountingSink(Logger logger) {
		super(new SinkFunction<T>() {

			@Override
			public void invoke(T value, Context ctx) throws Exception {
			}
		});
		this.logger = logger;
	}
	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		logger.warn("%{}%{}%{}%{}%{}%{}", "latency",
				System.currentTimeMillis() - latencyMarker.getMarkedTime(), System.currentTimeMillis(), latencyMarker.getMarkedTime(),
				latencyMarker.getSubtaskIndex(), getRuntimeContext().getIndexOfThisSubtask());
	}
}
