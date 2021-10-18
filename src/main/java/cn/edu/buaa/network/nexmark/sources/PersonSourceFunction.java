package cn.edu.buaa.network.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;

import java.util.Random;

public class PersonSourceFunction extends RichParallelSourceFunction<Person> {

	private volatile boolean running = true;
	private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
	private long eventsCountSoFar = 0;
	private final int rate;

	public PersonSourceFunction(int srcRate) {
		this.rate = srcRate;
	}

	@Override
	public void run(SourceContext<Person> sourceContext) throws Exception {
		while (running && eventsCountSoFar < 40_000_000) {
			long emitStartTime = System.currentTimeMillis();

			for (int i = 0; i < rate; i++) {
				long nextId = nextId();
				Random rnd = new Random(nextId);

				long eventTimestamp =
						config.timestampAndInterEventDelayUsForEvent(
								config.nextEventNumber(eventsCountSoFar)).getKey();

				sourceContext.collect(PersonGenerator.nextPerson(nextId, rnd, new DateTime(eventTimestamp), config));
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
