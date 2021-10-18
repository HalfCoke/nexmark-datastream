package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class Query5 {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();

		env.getConfig().setAutoWatermarkInterval(1000);

		final int srcRate = params.getInt("srcRate", 100000);

		DataStream<Bid> bidOneSource = env.addSource(new BidSourceFunction(srcRate))
				.name("BidOneSource")
				.uid("BidOneSource")
				.setParallelism(params.getInt("BidOneSource", 1))
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new TimestampExtractor(Time.seconds(0))));


		DataStream<Bid> bidTwoSource = env.addSource(new BidSourceFunction(srcRate))
				.name("BidTwoSource")
				.uid("BidTwoSource")
				.setParallelism(params.getInt("BidTwoSource", 1))
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new TimestampExtractor(Time.seconds(0))));



//		SELECT B1.auction, count(*) AS num
//      FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
//		GROUP BY B1.auction
		DataStream<Tuple3<Long, Long, Long>> bidOneWindowed = bidOneSource.keyBy((KeySelector<Bid, Long>) value -> value.auction)
				.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
				.apply(new CountBidsWithTime())
				.name("BidOneSlidingWindow")
				.uid("BidOneSlidingWindow")
				.setParallelism(params.getInt("BidOneSlidingWindow", 1));

//		ALL (SELECT B2.auction, count(*) AS num
//    	FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
//      GROUP BY B2.auction)
		DataStream<Tuple2<Long, Long>> bidTwoWindowed = bidTwoSource.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
				.apply(new MaxCountWithAllKeyWindow())
				.name("BidTwoSlidingWindow")
				.uid("BidTwoSlidingWindow")
				.setParallelism(params.getInt("BidTwoSlidingWindow", 1));


		KeyedStream<Tuple3<Long, Long, Long>, Long> keyedBidOneWindowed = bidOneWindowed.keyBy((KeySelector<Tuple3<Long, Long, Long>, Long>) value -> value.f2);
		KeyedStream<Tuple2<Long, Long>, Long> keyedBidTwoWindowed = bidTwoWindowed.keyBy((KeySelector<Tuple2<Long, Long>, Long>) value -> value.f1);

		SingleOutputStreamOperator<Tuple2<Long, Long>> process = keyedBidOneWindowed.connect(keyedBidTwoWindowed)
				.process(new OneAndTwoConnectProcess())
				.name("ConnectProcess")
				.uid("ConnectProcess")
				.setParallelism(params.getInt("ConnectProcess", 1));

		process.addSink(new DiscardingSink<>())
				.name("DiscardingSink")
				.uid("DiscardingSink")
				.setParallelism(params.getInt("DiscardingSink", 1));

		env.execute("Nexmark Query5");
	}

	public static final class OneAndTwoConnectProcess extends KeyedCoProcessFunction<Long, Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		ValueState<Long> maxCount;
		ListState<Tuple3<Long, Long, Long>> bidOneList;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Long> maxCountDescriptor = new ValueStateDescriptor<>("MaxCount", LONG_TYPE_INFO);
			ListStateDescriptor<Tuple3<Long, Long, Long>> bidOneListDescriptor = new ListStateDescriptor<>("BidOneList", TypeInformation.of(new TypeHint<Tuple3<Long, Long, Long>>() {
			}));

			maxCount = getRuntimeContext().getState(maxCountDescriptor);
			bidOneList = getRuntimeContext().getListState(bidOneListDescriptor);
		}

		@Override
		public void processElement1(Tuple3<Long, Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			Long count = maxCount.value();
			if (count == null) {
				bidOneList.add(value);
			} else if (value.f1 >= count) {
				out.collect(new Tuple2<>(value.f0, value.f1));
			}
		}

		@Override
		public void processElement2(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			maxCount.update(value.f0);
			Iterable<Tuple3<Long, Long, Long>> bidOnes = bidOneList.get();
			for (Tuple3<Long, Long, Long> bid : bidOnes) {
				if (bid.f1 >= value.f0) {
					out.collect(new Tuple2<>(bid.f0, bid.f1));
				}
			}
			bidOneList.clear();
		}
	}

	public static final class MaxCountWithAllKeyWindow extends RichAllWindowFunction<Bid, Tuple2<Long, Long>, TimeWindow> {

		MapState<Long, Long> keyedCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			MapStateDescriptor<Long, Long> keyedCountDescriptor = new MapStateDescriptor<>("KeyedCount", LONG_TYPE_INFO, LONG_TYPE_INFO);
			keyedCount = getRuntimeContext().getMapState(keyedCountDescriptor);
		}

		@Override
		public void apply(TimeWindow window, Iterable<Bid> values, Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Bid v : values) {
				Long acc = keyedCount.get(v.auction);
				Long count = acc == null ? 0L : acc;
				keyedCount.put(v.auction, count + 1);
			}
			Long maxCount = 0L;
			for (Long value : keyedCount.values()) {
				maxCount = Math.max(maxCount, value);
			}
			keyedCount.clear();
			out.collect(new Tuple2<>(maxCount, window.getEnd()));
		}
	}

	public static final class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Bid> {

		private long maxTimestamp = Long.MIN_VALUE;

		public TimestampExtractor(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(Bid element) {
			maxTimestamp = Math.max(maxTimestamp, element.dateTime.getMillis());
			return element.dateTime.getMillis();
		}
	}

	public static final class CountBidsWithTime extends RichWindowFunction<Bid, Tuple3<Long, Long, Long>, Long, TimeWindow> {

		@Override
		public void apply(Long aLong, TimeWindow window, Iterable<Bid> input, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
			Long count = 0L;
			for (Bid in : input) {
				count++;
			}
			out.collect(new Tuple3<>(aLong, count, window.getEnd()));

		}
	}
}
