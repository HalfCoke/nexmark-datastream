package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

/**
 * Query "11", 'User sessions' (Not in original suite.)
 *
 * <p>Group bids by the same user into sessions with {@code windowSizeSec} max gap. However limit
 * the session to at most {@code maxLogEvents}. Emit the number of bids per session.
 */
public class Query11 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();
		env.getConfig().setAutoWatermarkInterval(1000);

		final int srcRate = params.getInt("srcRate", 100000);

		SingleOutputStreamOperator<Bid> bids = env.addSource(new BidSourceFunction(srcRate))
				.setParallelism(params.getInt("Source", 1))
				.name("Source")
				.uid("Source")
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new Query5.TimestampExtractor(Time.seconds(0))));

		SingleOutputStreamOperator<Tuple2<Long, Long>> windowed = bids.keyBy((KeySelector<Bid, Long>) value -> value.bidder)
				.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
				.trigger(new MaxLogEventsTrigger())
				.aggregate(new CountBidsPerSession())
				.setParallelism(params.getInt("Window", 1))
				.name("Window")
				.uid("Window");

		windowed.addSink(new DiscardingSink<>())
				.name("DiscardingSink")
				.uid("DiscardingSink")
				.setParallelism(params.getInt("DiscardingSink", 1));

		env.execute("Nexmark Query11");

	}

	private static final class MaxLogEventsTrigger extends Trigger<Bid, TimeWindow> {

		private final long maxEvents = 100000L;

		private final ReducingStateDescriptor<Long> stateDesc =
				new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

		@Override
		public TriggerResult onElement(Bid element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
			ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
			count.add(1L);
			if (count.get() >= maxEvents) {
				count.clear();
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE_AND_PURGE;
		}

		@Override
		public boolean canMerge() {
			return true;
		}

		@Override
		public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
			ctx.mergePartitionedState(stateDesc);
		}

		@Override
		public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
			ctx.getPartitionedState(stateDesc).clear();
		}

		private static class Sum implements ReduceFunction<Long> {
			private static final long serialVersionUID = 1L;

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}

		}
	}

	private static final class CountBidsPerSession implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

		private long bidId = 0L;

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(Bid bid, Long accumulator) {
			bidId = bid.auction;
			return accumulator + 1;
		}

		@Override
		public Tuple2<Long, Long> getResult(Long accumulator) {
			return new Tuple2<>(bidId, accumulator);
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

}
