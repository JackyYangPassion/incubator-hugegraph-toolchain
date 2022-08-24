package com.baidu.hugegraph.loader.spark;

import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
//import com.baidu.hugegraph.backend.store.hbase.HbaseSerializer;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.structure.HugeEdge;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;

public class HugeGraphSparkLoaderHBase implements Serializable {
    static final Logger logger = LogManager.getLogger(HugeGraphSparkLoader.class);

    public static final String COL_FAMILY = "pd";
    public static final String COLUMN = "bf";
    private static final String APP_NAME = "FisherCoder-HFile-Generator";

    public static void main(String[]  args) throws Exception {
        HugeGraphSparkLoaderHBase hFileGenerator = new HugeGraphSparkLoaderHBase(); //生成HFile
        hFileGenerator.importIntoHBase(); //loade 到HBase
    }

    protected void importIntoHBase() throws Exception {
        JavaRDD<Row> rdd = readJsonTable();
        if (true) {
            hFilesToHBase(rdd);
        } else {
            putToHBase(rdd);
        }
    }

    /**
     * 核心步骤：
     * 1.读取 source 文件 序列化
     * 2.进行预分区
     * 3.生成HFile
     * 4.load 到HBase
     * @param rdd
     * @throws Exception
     */
    protected void hFilesToHBase(JavaRDD<Row> rdd) throws Exception {
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        String key = (String) row.get(0);
                        String value = (String) row.get(1);

//                        HugeConfig config = com.baidu.hugegraph.unit.FakeObjects.newConfig();
//                        config.setProperty("hbase.vertex_partitions",4);
//                        config.setProperty("hbase.enable_partition",true);
//                        config.setProperty("hbase.edge_partitions",4);
//
//                        BinarySerializer hbaseSer = new HbaseSerializer(config);
//                        HugeEdge edge = new com.baidu.hugegraph.unit.FakeObjects().newEdge("123", "456");
//                        BackendEntry entryVertex = hbaseSer.writeVertex(edge.sourceVertex());
//                        BackendEntry entryEdge = hbaseSer.writeEdge(edge);

                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
//                        byte[] rowKeyBytes = entryEdge.id().asBytes();
//                        rowKey.set(rowKeyBytes);
                        KeyValue keyValue = null;
//                        for(BackendEntry.BackendColumn column : entryEdge.columns()){
//                            keyValue = new KeyValue(rowKeyBytes,
//                                    Bytes.toBytes(COL_FAMILY),
//                                    column.name,
//                                    column.value);
//                        }

                        return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, keyValue);
                    }
                });

        Configuration baseConf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("test_salt1"));

        Job job = new Job(baseConf, APP_NAME);

        Partitioner partitioner = new HugeGraphSparkLoaderHBase.IntPartitioner(5);//预分区处理

        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd =
                javaPairRdd.repartitionAndSortWithinPartitions(partitioner);//精髓 partition内部做到有序


        HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor());

        System.out.println("Done configuring incremental load....");

        Configuration config = job.getConfiguration();
        FileSystem fs = FileSystem.get(config);
        String path  = fs.getWorkingDirectory().toString()+"/hfile-gen"+"/"+System.currentTimeMillis();//HFile 存储路径

        repartitionedRdd.saveAsNewAPIHadoopFile(
                path,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                config
        );
        System.out.println("Saved to HFiles to: " + path);

        if (true) {
            FsShell shell = new FsShell(conf);
            try {
                shell.run(new String[]{"-chmod", "-R", "777", path});
            } catch (Exception e) {
                System.out.println("Couldnt change the file permissions " + e
                        + " Please run command:"
                        + "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles "
                        + path + " '"
                        + "test" + "'\n"
                        + " to load generated HFiles into HBase table");
            }
            System.out.println("Permissions changed.......");
            loadHfiles(path);
            System.out.println("HFiles are loaded into HBase tables....");
        }
    }

    protected void loadHfiles(String path) throws Exception {
        Configuration baseConf = HBaseConfiguration.create();
        baseConf.set("hbase.zookeeper.quorum", "localhost");
        baseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(baseConf);
        Table table = conn.getTable(TableName.valueOf("test_salt1"));
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(baseConf);

        //TODO: load HFile to HBase
        loader.run(new String []{path, String.valueOf(table.getName())});

    }

    //精髓 repartitionAndSort
    public class IntPartitioner extends Partitioner {
        private final int numPartitions;
        public Map<List<String>, Integer> rangeMap = new HashMap<>();

        public IntPartitioner(int numPartitions) throws IOException {
            this.numPartitions = numPartitions;
            this.rangeMap = getRangeMap();
        }

        private Map<List<String>, Integer> getRangeMap() throws IOException {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            HRegionLocator locator = (HRegionLocator) admin.getConnection().getRegionLocator(TableName.valueOf("test_salt1"));

            Pair<byte[][], byte[][]> startEndKeys = locator.getStartEndKeys();

            Map<List<String>, Integer> rangeMap = new HashMap<>();
            for (int i = 0; i < startEndKeys.getFirst().length; i++) {
                String startKey = Bytes.toString(startEndKeys.getFirst()[i]);
                String endKey = Bytes.toString(startEndKeys.getSecond()[i]);
                System.out.println("startKey = " + startKey
                        + "\tendKey = " + endKey + "\ti = " + i);
                rangeMap.put(new ArrayList<>(Arrays.asList(startKey, endKey)), i);
            }
            return rangeMap;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            if (key instanceof ImmutableBytesWritable) {
                try {
                    ImmutableBytesWritable immutableBytesWritableKey = (ImmutableBytesWritable) key;
                    if (rangeMap == null || rangeMap.isEmpty()) {
                        rangeMap = getRangeMap();
                    }

                    String keyString = Bytes.toString(immutableBytesWritableKey.get());
                    for (List<String> range : rangeMap.keySet()) {
                        if (keyString.compareToIgnoreCase(range.get(0)) >= 0
                                && ((keyString.compareToIgnoreCase(range.get(1)) < 0)
                                || range.get(1).equals(""))) {
                            return rangeMap.get(range);
                        }
                    }
                    logger.error("Didn't find proper key in rangeMap, so returning 0 ...");
                    return 0;
                } catch (Exception e) {
                    logger.error("When trying to get partitionID, "
                            + "encountered exception: " + e + "\t key = " + key);
                    return 0;
                }
            } else {
                logger.error("key is NOT ImmutableBytesWritable type ...");
                return 0;
            }
        }
    }

    public JavaRDD<Row> readJsonTable() throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "true")
                .setAppName("spark-gen-hfile")
                .setMaster("local[*]");


        conf.registerKryoClasses(
                new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                        org.apache.hadoop.hbase.KeyValue.class,
                        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
                        Class.forName("scala.reflect.ClassTag$$anon$1")});

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> rows = null;
        String path = "file:///Users/yangjiaqi/Documents/learnStudent/ETLWork/test.csv";
        rows = spark.read().csv(path);
        return rows.toJavaRDD();
    }

    protected void putToHBase(JavaRDD<Row> rdd) throws IOException {
        Function2 writeToHBaseFunc = new Function2<Integer, Iterator<Row>, Iterator<Row>>() {
            @Override
            public Iterator<Row> call(Integer shardIndex, Iterator<Row> iterator)
                    throws Exception {
                put(shardIndex, iterator);
                return new ArrayList<Row>().iterator();
            }
        };

        Path tempOutputPath = Files.createTempDirectory("fishercoder")
                .toAbsolutePath();
        Files.deleteIfExists(tempOutputPath);

        try {
            rdd.repartition(1)//repartion  作用
                    .mapPartitionsWithIndex(writeToHBaseFunc, true)
                    .saveAsTextFile(tempOutputPath.toString());
        } finally {
            FileUtils.deleteDirectory(new File(tempOutputPath.toString()));
        }
    }

    protected void put(Integer shardIndex, Iterator<Row> iterator) throws IOException {
    }

}
