package cn.edu.buaa.network.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class BidSourceFunction extends RichParallelSourceFunction<Bid> {
	private volatile boolean running = true;
	private long eventsCountSoFar = 0;
	private final int rate;

	private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);

	public BidSourceFunction(int srcRate) {
		this.rate = srcRate;
	}

	@Override
	public void run(SourceContext<Bid> ctx) throws Exception {
		while (running && eventsCountSoFar < 20_000_000) {
			long emitStartTime = System.currentTimeMillis();

			for (int i = 0; i < rate; i++) {

				long nextId = nextId();
				Random rnd = new Random(nextId);

				// When, in event time, we should generate the event. Monotonic.
				long eventTimestamp =
						config.timestampAndInterEventDelayUsForEvent(
								config.nextEventNumber(eventsCountSoFar)).getKey();

				ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));
				eventsCountSoFar++;
			}

			// Sleep for the rest of timeslice if needed
			long emitTime = System.currentTimeMillis() - emitStartTime;
			if (emitTime < 1000) {
				Thread.sleep(1000 - emitTime);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	private long nextId() {
		return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
	}
}
