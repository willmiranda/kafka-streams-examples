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

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnrichedOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.microservices.Entry;
import io.confluent.examples.streams.avro.microservices.Event;
import io.confluent.examples.streams.avro.microservices.Floating;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Demonstrates how to perform joins between  KStreams and GlobalKTables, i.e. joins that
 * don't require re-partitioning of the input streams.
 * <p>
 * In this example, we join a stream of orders that reads from a topic named
 * "order" with a customers table that reads from a topic named "customer", and a products
 * table that reads fro a topic "product". The join produces an EnrichedOrder object.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic entries --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * $ bin/kafka-topics --create --topic floating --zookeeper localhost:2181 --partitions 3 --replication-factor 1
 * $ bin/kafka-topics --create --topic snowden.events --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.GlobalKTablesExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link GlobalKTablesExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. The driver will exit once it has received
 * # all EnrichedOrders
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.GlobalKTablesExampleDriver
 * }
 * </pre>
 * <p>
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class FloatingMonitoringTopology {

  static final String ENTRIES_TOPIC = "entries";
  static final String FLOATING_ACCOUNT_TOPIC = "floating";
  static final String FLOATING_ACCOUNT_STORE = "floating_store";
  static final String SNOWDEN_EVENTS_TOPIC = "snowden.events";

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    final KafkaStreams
        streams =
        createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams-global-tables");
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    streams.cleanUp();
    // start processing
    streams.start();
    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static KafkaStreams createStreams(final String bootstrapServers,
                                           final String schemaRegistryUrl,
                                           final String stateDir) {

    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "snowden.entries.app-1.0.0");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "snowden.entries.app-1.0.0.client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    // Set to earliest so we don't miss any data that arrived in the topics before the process
    // started
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create and configure the SpecificAvroSerdes required in this example
    final SpecificAvroSerde<Entry> entrySerde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig =
        Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl);
    entrySerde.configure(serdeConfig, false);

    final SpecificAvroSerde<Floating> floatingSerde = new SpecificAvroSerde<>();
    floatingSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<Event> snowdenEntriesSerde = new SpecificAvroSerde<>();
    snowdenEntriesSerde.configure(serdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    // Get the stream of orders
    final KStream<Long, Entry> entriesStream = builder.stream(ENTRIES_TOPIC, Consumed.with(Serdes.Long(), entrySerde));

    // Create a global table for customers. The data from this global table
    // will be fully replicated on each instance of this application.
    final GlobalKTable<String, Floating>
        floatingAccounts =
        builder.globalTable(FLOATING_ACCOUNT_TOPIC,
                Materialized.<String, Floating, KeyValueStore<Bytes, byte[]>>as(FLOATING_ACCOUNT_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(floatingSerde));

    // Join the orders stream to the customer global table. As this is global table
    // we can use a non-key based join with out needing to repartition the input stream
    final KStream<Long, Event> eventsStream = entriesStream.join(floatingAccounts,
                                                                (entryId, entry) -> entry.getMoipAccount(),
                                                                (entry, floating) -> new Event( entry.getEventId(),
                                                                                                entry.getMoipAccount(),
                                                                                                entry.getFloating(),
                                                                                                floating.getFloating(),
                                                                                                entry.getPaymentForm()
                                                                ));

    // Join the enriched customer order stream with the product global table. As this is global table
    // we can use a non-key based join without needing to repartition the input stream
//    final KStream<Long, EnrichedOrder> enrichedOrdersStream = customerOrdersStream.join(products,
//                                                                        (orderId, customerOrder) -> customerOrder
//                                                                            .productId(),
//                                                                        (customerOrder, product) -> new EnrichedOrder(
//                                                                            product,
//                                                                            customerOrder.customer,
//                                                                            customerOrder.order));

    // write the enriched order to the enriched-order topic
    eventsStream.to(SNOWDEN_EVENTS_TOPIC, Produced.with(Serdes.Long(), snowdenEntriesSerde));

    return new KafkaStreams(builder.build(), new StreamsConfig(streamsConfiguration));
  }


  // Helper class for intermediate join between
  // orders & customers
  private static class CustomerOrder {
    private final Customer customer;
    private final Order order;

    CustomerOrder(final Customer customer, final Order order) {
      this.customer = customer;
      this.order = order;
    }

    long productId() {
      return order.getProductId();
    }

  }
}
