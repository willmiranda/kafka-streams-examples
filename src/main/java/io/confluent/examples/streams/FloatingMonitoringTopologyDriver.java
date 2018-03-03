/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.microservices.Entry;
import io.confluent.examples.streams.avro.microservices.Event;
import io.confluent.examples.streams.avro.microservices.Floating;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.*;

import static io.confluent.examples.streams.FloatingMonitoringTopology.ENTRIES_TOPIC;
import static io.confluent.examples.streams.FloatingMonitoringTopology.FLOATING_ACCOUNT_TOPIC;
import static io.confluent.examples.streams.FloatingMonitoringTopology.SNOWDEN_EVENTS_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.*;

/**
 * This is a sample driver for the {@link PageViewRegionExample} and {@link PageViewRegionLambdaExample}.
 * To run this driver please first refer to the instructions in {@link PageViewRegionExample} or {@link PageViewRegionLambdaExample}.
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.PageViewRegionExampleDriver
 * }
 * </pre>
 * You should terminate with {@code Ctrl-C}.
 */
public class FloatingMonitoringTopologyDriver {

  private static final Random RANDOM = new Random();
  private static final int RECORDS_TO_GENERATE = 100;

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    generateFloatings(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
    generateEntries(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE);
    receiveSnowdenEntries(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
  }

  private static void receiveSnowdenEntries(final String bootstrapServers,
                                            final String schemaRegistryUrl,
                                            final int expected) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    final KafkaConsumer<Long, Event> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singleton(SNOWDEN_EVENTS_TOPIC));
    int received = 0;
    while(received < expected) {
      final ConsumerRecords<Long, Event> records = consumer.poll(Long.MAX_VALUE);
      records.forEach(record -> System.out.println(record.value()));
    }
    consumer.close();
  }

  static List<Floating> generateFloatings(final String bootstrapServers,
                                          final String schemaRegistryUrl,
                                          final int count) {

    final SpecificAvroSerde<Floating> floatingSerde = createSerde(schemaRegistryUrl);
    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<String, Floating>
            producer =
            new KafkaProducer<>(producerProperties, Serdes.String().serializer(), floatingSerde.serializer());
    final List<Floating> allFloatings = new ArrayList<>();
    final String [] accounts = { "MPA-1", "MPA-2", "MPA-3" };
    final String [] paymentForms = { "CREDIT", "DEBIT" };
    final Random random = new Random();
    for(long i = 0; i < count; i++) {
      String account = accounts[random.nextInt(accounts.length)];
      final Floating floating = new Floating(randomString(10),
              account,
              random.nextInt(20),
              paymentForms[random.nextInt(paymentForms.length)]);
      allFloatings.add(floating);
      producer.send(new ProducerRecord<>(FLOATING_ACCOUNT_TOPIC, account, floating));
    }
    producer.close();
    return allFloatings;
  }

  static List<Entry> generateEntries(final String bootstrapServers,
                                     final String schemaRegistryUrl,
                                     final int numCustomers,
                                     final int count) {

    final SpecificAvroSerde<Entry> entriesSerde = createSerde(schemaRegistryUrl);

    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<Long, Entry>
            producer =
            new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), entriesSerde.serializer());

    final String [] status = { "SCHEDULED", "SETTLED" };
    final String [] types = { "CREDIT", "DEBIT" };
    final String [] paymentForms = { "CREDIT", "DEBIT" };
    final String [] events = { "PAY-1", "PAY-2" };
    final String [] accounts = { "MPA-1", "MPA-2", "MPA-3" };

    final List<Entry> allEntries = new ArrayList<>();
    for(long i = 0; i < count; i++) {
      final Entry entry = new Entry(randomString(20),
              RANDOM.nextLong(),
              types[RANDOM.nextInt(types.length)],
              paymentForms[RANDOM.nextInt(paymentForms.length)],
              status[RANDOM.nextInt(status.length)],
              RANDOM.nextInt(30),
              events[RANDOM.nextInt(events.length)],
              accounts[RANDOM.nextInt(accounts.length)]);
      allEntries.add(entry);
      producer.send(new ProducerRecord<>(ENTRIES_TOPIC, i, entry));
    }
    producer.close();
    return allEntries;
  }

  private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl) {

    final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    serde.configure(serdeConfig, false);
    return serde;
  }

  // Copied from org.apache.kafka.test.TestUtils
  private static String randomString(int len) {
    final StringBuilder b = new StringBuilder();

    for(int i = 0; i < len; ++i) {
      b.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".charAt(RANDOM.nextInt
              ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".length())));
    }

    return b.toString();
  }

}
