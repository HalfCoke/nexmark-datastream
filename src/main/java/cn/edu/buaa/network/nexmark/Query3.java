package cn.edu.buaa.network.nexmark;

import cn.edu.buaa.network.nexmark.sources.AuctionSourceFunction;
import cn.edu.buaa.network.nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import java.util.HashSet;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

/**
 * Query 3, 'Local Item Suggestion'. Who is selling in OR, ID or CA in category 10, and for what
 * auction ids? In CQL syntax:
 *
 * <pre>
 * SELECT Istream(P.name, P.city, P.state, A.id)
 * FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
 * WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category
 * = 10;
 * </pre>
 *
 * <p>We'll implement this query to allow 'new auction' events to come before the 'new person'
 * events for the auction seller. Those auctions will be stored until the matching person is seen.
 * Then all subsequent auctions for a person will use the stored person record.
 *
 * <p>A real system would use an external system to maintain the id-to-person association.
 */

public class Query3 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.disableOperatorChaining();

		final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

		final int personSrcRate = params.getInt("person-srcRate", 10000);

		DataStream<Auction> auctionsSource = env.addSource(new AuctionSourceFunction(auctionSrcRate))
				.name("AuctionsSource")
				.uid("AuctionsSource")
				.setParallelism(params.getInt("AuctionsSource", 1));

		DataStream<Auction> filteredAuction = auctionsSource.filter((FilterFunction<Auction>) auction -> auction.category == 10)
				.name("AuctionFilter")
				.uid("AuctionFilter")
				.setParallelism(params.getInt("AuctionFilter", 1));

		DataStream<Person> personSource = env.addSource(new PersonSourceFunction(personSrcRate))
				.name("PersonSource")
				.uid("PersonSource")
				.setParallelism(params.getInt("PersonSource", 1));

		DataStream<Person> filteredPerson = personSource.filter((FilterFunction<Person>) person -> (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA")))
				.name("PersonFilter")
				.uid("PersonFilter")
				.setParallelism(params.getInt("PersonFilter", 1));

		KeyedStream<Auction, Long> auctionKeyedStream = filteredAuction.keyBy((KeySelector<Auction, Long>) auction -> auction.seller);

		KeyedStream<Person, Long> personLongKeyedStream = filteredPerson.keyBy((KeySelector<Person, Long>) person -> person.id);

		DataStream<Tuple4<String, String, String, Long>> connect = auctionKeyedStream.connect(personLongKeyedStream)
				.flatMap(new JoinPersonsWithAuctions())
				.name("IncrementalJoin")
				.uid("IncrementalJoin")
				.setParallelism(params.getInt("IncrementalJoin", 1));

		connect.addSink(new DiscardingSink<>())
				.name("DiscardingSink")
				.uid("DiscardingSink")
				.setParallelism(params.getInt("DiscardingSink", 1));

		env.execute("NexMark Query3");
	}

	private static class JoinPersonsWithAuctions extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {

		// person state: id, <name, city, state>
		MapState<Long, Tuple3<String, String, String>> personMapState;

		// auction state: seller, List<id>
		MapState<Long, HashSet<Long>> auctionMapState;

		@Override
		public void open(Configuration parameters) throws Exception {
			MapStateDescriptor<Long, Tuple3<String, String, String>> personMapStateDescriptor = new MapStateDescriptor<>(
					"PersonMapState", LONG_TYPE_INFO, new TupleTypeInfo<>(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO)
			);

			MapStateDescriptor<Long, HashSet<Long>> auctionMapStateDescriptor = new MapStateDescriptor<>(
					"AuctionMapState", LONG_TYPE_INFO, TypeInformation.of(new TypeHint<HashSet<Long>>() {
			}));

			personMapState = getRuntimeContext().getMapState(personMapStateDescriptor);
			auctionMapState = getRuntimeContext().getMapState(auctionMapStateDescriptor);
		}

		@Override
		public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
			if (personMapState.contains(auction.seller)) {
				Tuple3<String, String, String> match = personMapState.get(auction.seller);
				collector.collect(new Tuple4<>(match.f0, match.f1, match.f2, auction.id));
			} else {
				if (auctionMapState.contains(auction.seller)) {
					HashSet<Long> ids = auctionMapState.get(auction.seller);
					ids.add(auction.id);
					auctionMapState.put(auction.seller, ids);
				}
			}
		}

		@Override
		public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
			personMapState.put(person.id, new Tuple3<>(person.name, person.city, person.state));

			if (auctionMapState.contains(person.id)) {
				HashSet<Long> ids = auctionMapState.get(person.id);
				auctionMapState.remove(person.id);
				for (Long id : ids) {
					collector.collect(new Tuple4<>(person.name, person.city, person.state, id));
				}
			}
		}
	}
}
