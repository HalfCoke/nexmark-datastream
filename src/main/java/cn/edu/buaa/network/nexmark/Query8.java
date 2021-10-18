package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.AuctionSourceFunction;
import cn.edu.buaa.network.nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

/**
 * Query 8, 'Monitor New Users'. Select people who have entered the system and created auctions in
 * the last 12 hours, updated every 12 hours. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(P.id, P.name, A.reserve)
 * FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
 * WHERE P.id = A.seller;
 * </pre>
 *
 * <p>To make things a bit more dynamic and easier to test we'll use a much shorter window.
 */
public class Query8 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();
		env.getConfig().setAutoWatermarkInterval(1000);
		env.setParallelism(params.getInt("Window", 1));

		final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

		final int personSrcRate = params.getInt("person-srcRate", 10000);

		DataStream<Auction> auctionSource = env.addSource(new AuctionSourceFunction(auctionSrcRate))
				.name("AuctionSource")
				.uid("AuctionSource")
				.setParallelism(params.getInt("AuctionSource", 1))
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new AuctionTimestampExtractor(Time.seconds(0))));


		DataStream<Person> personSource = env.addSource(new PersonSourceFunction(personSrcRate))
				.name("PersonSource")
				.uid("PersonSource")
				.setParallelism(params.getInt("PersonSource", 1))
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new PersonTimestampExtractor(Time.seconds(0))));


		DataStream<Tuple3<Long, String, Long>> apply = auctionSource.join(personSource)
				.where((KeySelector<Auction, Long>) value -> value.seller)
				.equalTo((KeySelector<Person, Long>) value -> value.id)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.apply(new FlatJoinFunction<Auction, Person, Tuple3<Long, String, Long>>() {
					@Override
					public void join(Auction a, Person p, Collector<Tuple3<Long, String, Long>> out) throws Exception {
						out.collect(new Tuple3<>(p.id, p.name, a.reserve));
					}
				});


		apply.addSink(new DiscardingSink<>())
				.name("DiscardingSink")
				.uid("DiscardingSink")
				.setParallelism(params.getInt("DiscardingSink", 1));

		env.execute("Nexmark Query8");
	}

	private static final class AuctionTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Auction> {

		private long maxTimestamp = Long.MIN_VALUE;

		public AuctionTimestampExtractor(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(Auction element) {
			maxTimestamp = Math.max(maxTimestamp, element.dateTime.getMillis());
			return element.dateTime.getMillis();
		}
	}

	private static final class PersonTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Person> {

		private long maxTimestamp = Long.MIN_VALUE;

		public PersonTimestampExtractor(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(Person element) {
			maxTimestamp = Math.max(maxTimestamp, element.dateTime.getMillis());
			return element.dateTime.getMillis();
		}
	}
}
