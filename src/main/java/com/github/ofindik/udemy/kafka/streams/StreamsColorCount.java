package com.github.ofindik.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsColorCount {
	public static void main (String[] args) {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "streams-color-app");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		// we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
		config.put (StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		StreamsBuilder streamsBuilder = new StreamsBuilder ();
		// 1 - Read one topic from Kafka (KStream)
		KStream<String, String> textLines = streamsBuilder.stream ("favorite-color-input");
		// 2 - Filter bad values
		textLines.filter ((key, value) -> value.contains (","))
			// 3 - SelectKey that will be the user id
			.selectKey ((key, value) -> value.split (",")[0].toLowerCase ())
			// 4 - MapValues to extract the color
			.mapValues (value -> value.split (",")[1].toLowerCase ())
			// 5 - Filter to remove bad colors
			.filter ((user, color) -> Arrays.asList ("red", "blue", "green").contains (color))
			// 6 - Write to Kafka as intermediary topic
			.to ("favorite-color-intermediary-topic");

		// 7 - Read from kafka as KTable
		KTable<String, String> usersAndColoursTable = streamsBuilder.table ("favorite-color-intermediary-topic");

		KTable<String, Long> colorCountTable = usersAndColoursTable
			// 8 - GroupBy colors
			.groupBy ((user, color) -> new KeyValue<> (color, color))
			// 9 - Count to count colors occurance
			.count (Named.as ("CountsByColours"));
		// 10 - Write to Kafka as final topic
		colorCountTable.toStream ().to ("favorite-color-output", Produced.with (Serdes.String (), Serdes.Long ()));

		KafkaStreams kafkaStreams = new KafkaStreams (streamsBuilder.build (), config);
		kafkaStreams.start ();

		// printed the topology
		System.out.println (kafkaStreams.toString ());

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime ().addShutdownHook (new Thread (kafkaStreams::close));
	}
}
