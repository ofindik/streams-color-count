package com.github.ofindik.udemy.kafka.stream;

import com.github.ofindik.udemy.kafka.streams.StreamsColorCount;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class StreamsColorCountTest {

	TopologyTestDriver testDriver;

	@Before
	public void setUpTopologyTestDriver () {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		StreamsColorCount streamsColorCount = new StreamsColorCount ();
		Topology topology = streamsColorCount.createTopology ();
		testDriver = new TopologyTestDriver (topology, config);
	}

	@After
	public void closeTestDriver () {
		testDriver.close ();
	}

	@Test
	public void dummyTest () {
		String dummy = "Du" + "mmy";
		assertEquals (dummy, "Dummy");
	}

	@Test
	public void makeSureCountsAreCorrect () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("favorite-color-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, String> intermediaryTopic = testDriver.createOutputTopic ("favorite-color-intermediary-topic",
			new StringDeserializer (), new StringDeserializer ());
		TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic ("favorite-color-output",
			new StringDeserializer (), new LongDeserializer ());

		pushNewInputRecord (inputTopic, "osman,red");
		pushNewInputRecord (inputTopic, "hasan,red");

		assertThat (intermediaryTopic.readKeyValue (), equalTo (new KeyValue<> ("osman", "red")));
		assertThat (intermediaryTopic.readKeyValue (), equalTo (new KeyValue<> ("hasan", "red")));

		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("red", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("red", 2L)));
	}

	@Test
	public void makeSureColorChangesAreCountedProperly () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("favorite-color-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, String> intermediaryTopic = testDriver.createOutputTopic ("favorite-color-intermediary-topic",
			new StringDeserializer (), new StringDeserializer ());
		TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic ("favorite-color-output",
			new StringDeserializer (), new LongDeserializer ());

		pushNewInputRecord (inputTopic, "osman,red");
		pushNewInputRecord (inputTopic, "hasan,blue");
		pushNewInputRecord (inputTopic, "osman,blue");

		assertThat (intermediaryTopic.readKeyValue (), equalTo (new KeyValue<> ("osman", "red")));
		assertThat (intermediaryTopic.readKeyValue (), equalTo (new KeyValue<> ("hasan", "blue")));
		assertThat (intermediaryTopic.readKeyValue (), equalTo (new KeyValue<> ("osman", "blue")));

		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("red", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("blue", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("red", 0L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("blue", 2L)));
	}

	private void pushNewInputRecord (TestInputTopic<String, String> inputTopic, String value) {
		inputTopic.pipeInput (null, value);
	}
}
