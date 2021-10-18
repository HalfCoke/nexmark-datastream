package cn.edu.buaa.network.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class AuctionSourceFunction extends RichParallelSourceFunction<Auction> {
	private volatile boolean running = true;
	private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
	private long eventsCountSoFar = 0;
	private final int rate;

	public AuctionSourceFunction(int srcRate) {
		this.rate = srcRate;
	}


	@Override
	public void run(SourceContext<Auction> sourceContext) throws Exception {
		while (running && eventsCountSoFar < 70_000_000) {
			long emitStartTime = System.currentTimeMillis();

			for (int i = 0; i < rate; i++) {
				long nextId = nextId();
				Random rnd = new Random(nextId);

				long eventTimestamp =
						config.timestampAndInterEventDelayUsForEvent(
								config.nextEventNumber(eventsCountSoFar)).getKey();

				sourceContext.collect(AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config));
				eventsCountSoFar++;
			}

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
