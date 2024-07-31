package com.shapira.examples.streams.stockstats;

import com.shapira.examples.streams.stockstats.serde.JsonDeserializer;
import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.shapira.examples.streams.stockstats.serde.WrapperSerde;
import com.shapira.examples.streams.stockstats.model.Trade;
import com.shapira.examples.streams.stockstats.model.TradeStats;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

/**
 * Input is a stream of trades
 * Output is two streams: One with minimum and avg "ASK" price for every 10 seconds window
 * Another with the top-3 stocks with lowest minimum ask every minute
 */
public class StockStatsExample {

    public static void main(String[] args) throws Exception {

        Properties props;
        if (args.length==1)
            props = LoadConfigs.loadConfig(args[0]);
        else
            props = LoadConfigs.loadConfig();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Time interval, in millisecond, for our aggregation window
        long windowSize = 5000;

        // creating an AdminClient and checking the number of brokers in the cluster, so I'll know how many replicas we want...
        // AdminClient是Kafka提供的用于管理和监控Kafka集群的工具。它允许用户以编程方式执行各种管理操作，如创建、删除和列出主题，修改主题配置，查看集群信息等。
        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor",clusterSize);
        else
            props.put("replication.factor",3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<Windowed<String>, TradeStats> stats = source
                /*
                 * groupByKey() 会将一个 KStream 按照 key 分组，生成一个 KGroupedStream，按key聚合操作的前提步骤，
                 * KGroupedStream 表示按 key 进行分组的记录流，后续可以对这个分组进行聚合、计数、减少等操作。
                 */
                .groupByKey()
                /*
                 * 创建窗口操作：
                 * windowedBy(TimeWindows.of(Duration.ofMillis(windowSize))：定义一个windowSize即5000毫秒的移动窗口
                 * advanceBy(Duration.ofSeconds(1)))：定义滑动的步长，1秒钟，即窗口每1秒向前滑动一次，每隔1秒钟，就会创建一个新的窗口，且每个窗口会重叠。
                 */
                .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize)).advanceBy(Duration.ofSeconds(1)))
                /*
                 * (k, v, tradestats) -> tradestats.add(v)：定义聚合器将每个新记录合并到现有的聚合结果中
                 * Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")：定义聚合结果的存储方式和存储位置
                 * Materialized叫做物化视图，存储在Kafka Streams内置的键值（KV）好像是rocksdb存储中。
                 * 具体来说，它存储在一个 WindowStore 中，这是 Kafka Streams 提供的一种内置的状态存储机制。
                 * as("trade-aggregates")就是指定存储的名称方便检索和获取
                 * 最终aggregate方法会得到一个 KTable<Windowed<K>, VR>，KTable 内部使用的是一种类似于键值存储的数据结构，并且实现通常是内嵌的RocksDB数据库。
                 * RocksDB：一个高性能的嵌入式键值存储库，特别适合于在磁盘上存储大量数据，并支持快速的读写操作。
                 */
                .aggregate(TradeStats::new,(k, v, tradestats) -> tradestats.add(v),
                        Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
                                .withValueSerde(new TradeStatsSerde()))
                /*
                 * toStream() 将KTable转换为KStream。在 Kafka Streams中，KTable表示的是一个表结构的数据，包含的是最新的状态。
                 * 而 KStream 表示的是一个流结构的数据，包含的是所有的事件
                 */
                .toStream()
                /*
                 * mapValues() 于对流中的每个值进行转换。mapValues会保留原来的key，只对value进行转换，这里就是转化位交易的平均价格
                 */
                .mapValues(TradeStats::computeAvgPrice);

        /*
         * to就是KStream数据写入到新的Kafka topic "stockstats-output" 中
         * Produced是个配置生产者的属性的工具类，这里配置了处理时间窗口的 Serde 工具类
         */
        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize)));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
        }
    }

}
