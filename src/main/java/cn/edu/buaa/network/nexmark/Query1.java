package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;


/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros. In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 */
public class Query1 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final int srcRate = params.getInt("srcRate", 100000);

		final float exchangeRate = 0.82F;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();

		env.getConfig().setLatencyTrackingInterval(5000);

		DataStream<Bid> bids = env.addSource(new BidSourceFunction(srcRate))
				.setParallelism(params.getInt("Source", 1))
				.name("Source")
				.uid("Source");

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Long>> mapper = bids.map(new MapFunction<Bid, Tuple4<Long, Long, Long, Long>>() {
			@Override
			public Tuple4<Long, Long, Long, Long> map(Bid bid) throws Exception {
				return new Tuple4<>(bid.auction, dollarToEuro(bid.price, exchangeRate), bid.bidder, bid.dateTime.getMillis());
			}
		})
				.setParallelism(params.getInt("Map", 1))
				.name("Map")
				.uid("Map");

		mapper.addSink(new DiscardingSink<>())
				.setParallelism(params.getInt("DiscardingSink", 1))
				.name("DiscardingSink")
				.uid("DiscardingSink");

		env.execute("Nexmark Query1");

	}

	private static long dollarToEuro(long dollarPrice, float rate) {
		return (long) (rate * dollarPrice);
	}
}
