package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

/**
 * Query 2, 'Filtering. Find bids with specific auction ids and show their bid price. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(auction, price)
 * FROM Bid [NOW]
 * WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
 * </pre>
 */

public class Query2 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();

		final int srcRate = params.getInt("srcRate", 100000);

		DataStream<Bid> bids = env.addSource(new BidSourceFunction(srcRate))
				.name("Source")
				.uid("Source")
				.setParallelism(params.getInt("Source", 1));

		SingleOutputStreamOperator<Tuple2<Long, Long>> flatMap = bids.flatMap(new FlatMapFunction<Bid, Tuple2<Long, Long>>() {
			@Override
			public void flatMap(Bid bid, Collector<Tuple2<Long, Long>> collector) throws Exception {
				if (bid.auction % 1007 == 0 || bid.auction % 1020 == 0 || bid.auction % 2001 == 0 || bid.auction % 2019 == 0 || bid.auction % 2087 == 0) {
					collector.collect(new Tuple2<>(bid.auction, bid.price));
				}
			}
		})
				.setParallelism(params.getInt("FlatMap", 1))
				.name("FlatMap")
				.uid("FlatMap");

		flatMap.addSink(new DiscardingSink<>())
				.setParallelism(params.getInt("DiscardingSink", 1))
				.name("DiscardingSink")
				.uid("DiscardingSink");

		env.execute("NexMark Query2");
	}
}
