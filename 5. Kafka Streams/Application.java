import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;


public class Application {

	public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);
		//props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> studentStream = builder.stream(studentTopic),
								classroomStream = builder.stream(classroomTopic);
		KTable<String, String> studentClassroomMapping = studentStream
			.groupByKey().reduce((aggr, result) -> result);
		KTable<String, Long> classroomStudentsMapping = studentClassroomMapping
			.groupBy((student, room) ->  new KeyValue<>(room, student))
			.count();
		KTable<String, String> classroomMaxStudentsMapping = classroomStream
			.groupByKey().reduce((aggr, result) ->  result);
		KTable<String, String> studentsInClassrooms = classroomStudentsMapping
			.join(classroomMaxStudentsMapping, (numStudents, maxStudents) -> {
				return numStudents.toString() + "," + maxStudents.toString();
			});
		KTable<String, String> output = studentsInClassrooms.toStream()
			.groupByKey().aggregate(
				() -> null,
				(identifier, result, input) -> {
					int students = Integer.parseInt(result.split(",")[0]);
					if (students > Integer.parseInt(result.split(",")[1])) {
						return String.valueOf(students);
					} else if (StringUtils.isNumeric(input)) {
						return "OK";
					} else {
						return null;
					}
				}
			);

		Serde<String> stringsToOutput = Serdes.String();

		output.toStream().filter((identifier, result) -> result != null)
			.to(outputTopic, Produced.with(stringsToOutput, stringsToOutput));
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
