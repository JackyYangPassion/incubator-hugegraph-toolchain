package com.baidu.hugegraph.loader.direct.util;

import com.baidu.hugegraph.loader.builder.EdgeBuilder;
import com.baidu.hugegraph.loader.builder.ElementBuilder;
import com.baidu.hugegraph.loader.builder.VertexBuilder;
import com.baidu.hugegraph.loader.executor.LoadContext;
import com.baidu.hugegraph.loader.executor.LoadOptions;
import com.baidu.hugegraph.loader.mapping.EdgeMapping;
import com.baidu.hugegraph.loader.mapping.ElementMapping;
import com.baidu.hugegraph.loader.mapping.InputStruct;
import com.baidu.hugegraph.loader.mapping.VertexMapping;
import com.baidu.hugegraph.loader.source.file.FileSource;
import com.baidu.hugegraph.structure.GraphElement;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.util.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SinkToHBase implements Serializable {
    public final Logger LOG = Log.logger(SinkToHBase.class);
    public final String COL_FAMILY = "f";
    private final String APP_NAME = "FisherCoder-HFile-Generator";
    private static HBaseSerializer serializer;
    public SinkToHBase(HBaseSerializer serializer ){
        this.serializer = serializer;
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
    public void hFilesToHBase(JavaRDD<Row> rdd, InputStruct struct, LoadOptions loadOptions) throws Exception {

        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        /**
                         * 核心逻辑：
                         * Row ---> Vertex/Edge ----> KV
                         */
                        List<GraphElement> elementsElement;//存储返回的  GraphElement: Vertex/Edge

                        Map<ElementBuilder, List<GraphElement>> buildersForGraphElement = new HashMap<>();

                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
                        KeyValue keyValue = null;

                        LoadContext context = new LoadContext(loadOptions);
                        for (VertexMapping vertexMapping : struct.vertices()) {
                            buildersForGraphElement.put(
                                    new VertexBuilder(context, struct, vertexMapping),
                                    new ArrayList<>());
                        }
                        for (EdgeMapping edgeMapping : struct.edges()) {
                            buildersForGraphElement.put(new EdgeBuilder(context, struct, edgeMapping),
                                    new ArrayList<>());
                        }

                        /**
                         * 构建 vertex/edge --->  K-V
                         */
                        for (Map.Entry<ElementBuilder, List<GraphElement>> builderMap : buildersForGraphElement.entrySet()) {// 控制类型 点/边
                            ElementMapping elementMapping = builderMap.getKey().mapping();
                            if (elementMapping.skip()) {
                                continue;
                            }
                            ElementBuilder builder = builderMap.getKey();
                            if ("".equals(row.mkString())) {
                                break;
                            }
                            switch (struct.input().type()) {
                                case FILE:
                                case HDFS:
                                    FileSource fileSource = struct.input().asFileSource();
                                    elementsElement = builder.build(fileSource.header(),
                                            row.mkString().split(fileSource.delimiter()));//此处 只要传递的是对应的字符数组即可
                                    break;
                                default:
                                    throw new AssertionError(String.format(
                                            "Unsupported input source '%s'",
                                            struct.input().type()));
                            }

                            boolean isVertex = builder.mapping().type().isVertex();
                            if (isVertex) {
                                for (Vertex vertex : (List<Vertex>) (Object) elementsElement) {
                                    byte[] rowkey = serializer.getKeyBytes(vertex);
                                    byte[] values = serializer.getValueBytes(vertex);
                                    rowKey.set(rowkey);
                                    keyValue = new KeyValue(rowkey,
                                            Bytes.toBytes(COL_FAMILY),
                                            Bytes.toBytes(""),
                                            values);
                                }
                            } else {
                                for (Edge edge : (List<Edge>) (Object) elementsElement) {
                                    byte[] rowkey = serializer.getKeyBytes(edge);
                                    byte[] values = serializer.getValueBytes(edge);
                                    rowKey.set(rowkey);
                                    keyValue = new KeyValue(rowkey,
                                            Bytes.toBytes(COL_FAMILY),
                                            Bytes.toBytes(""),
                                            values);
                                }
                            }
                        }
                        return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, keyValue);
                    }
                });

        String tableName = null;
        int partition = 0;
        String HFilePath;
        if( struct.edges().size() > 0){//倒入的是边  表名换成 出边表
            System.out.println("this is edge struct: " + struct.edges().size());
            tableName = "hugegraph12p:g_oe";
            partition = serializer.getEdgeLogicPartitions();
        }else if ( struct.vertices().size() > 0 ) {
            System.out.println("this is vertices struct: " + struct.vertices().size());
            tableName = "hugegraph12p:g_v";
            partition = serializer.getVertexLogicPartitions();
        }
        Configuration baseConf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Job job = new Job(baseConf, APP_NAME);

        Partitioner partitioner = new SinkToHBase.IntPartitioner(partition, tableName);//预分区处理

        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd = javaPairRdd.repartitionAndSortWithinPartitions(partitioner);//精髓 partition内部做到有序


        HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor());

        System.out.println("Done configuring incremental load....");

        Configuration config = job.getConfiguration();
        FileSystem fs = FileSystem.get(config);
        String path = fs.getWorkingDirectory().toString() + "/hfile-gen" + "/" + System.currentTimeMillis();//HFile 存储路径

        /**
         * 生成 HFile
         */
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
            loadHfiles(path, tableName);// BulkLoad HFile to HBase
            System.out.println("HFiles are loaded into HBase tables....");
        }
    }

    protected void loadHfiles(String path, String tableName) throws Exception {
        Configuration baseConf = HBaseConfiguration.create();
        baseConf.set("hbase.zookeeper.quorum", "localhost");
        baseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(baseConf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(baseConf);

        //TODO: load HFile to HBase
        loader.run(new String []{path, String.valueOf(table.getName())});

    }

    //精髓 repartitionAndSort
    public class IntPartitioner extends Partitioner {
        private final int numPartitions;
        public Map<List<String>, Integer> rangeMap = new HashMap<>();
        private String tableName;

        public IntPartitioner(int numPartitions, String tableName) throws IOException {
            this.numPartitions = numPartitions;
            this.rangeMap = getRangeMap(tableName);
            this.tableName = tableName;
        }

        private Map<List<String>, Integer> getRangeMap(String tableName) throws IOException {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            HRegionLocator locator = (HRegionLocator) admin.getConnection().getRegionLocator(TableName.valueOf(tableName));

            Pair<byte[][], byte[][]> startEndKeys = locator.getStartEndKeys();

            Map<List<String>, Integer> rangeMap = new HashMap<>();
            for (int i = 0; i < startEndKeys.getFirst().length; i++) {
                String startKey = Bytes.toString(startEndKeys.getFirst()[i]);
                String endKey = Bytes.toString(startEndKeys.getSecond()[i]);
//                System.out.println("startKey = " + startKey
//                        + "\tendKey = " + endKey + "\ti = " + i);
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
                        rangeMap = getRangeMap(this.tableName);
                    }

                    String keyString = Bytes.toString(immutableBytesWritableKey.get());
                    for (List<String> range : rangeMap.keySet()) {
                        if (keyString.compareToIgnoreCase(range.get(0)) >= 0
                                && ((keyString.compareToIgnoreCase(range.get(1)) < 0)
                                || range.get(1).equals(""))) {
                            return rangeMap.get(range);
                        }
                    }
                    LOG.error("Didn't find proper key in rangeMap, so returning 0 ...");
                    return 0;
                } catch (Exception e) {
                    LOG.error("When trying to get partitionID, "
                            + "encountered exception: " + e + "\t key = " + key);
                    return 0;
                }
            } else {
                LOG.error("key is NOT ImmutableBytesWritable type ...");
                return 0;
            }
        }
    }

}
