/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.loader.spark;

import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.hbase.HbaseSerializer;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.loader.builder.EdgeBuilder;
import com.baidu.hugegraph.loader.builder.ElementBuilder;
import com.baidu.hugegraph.loader.builder.VertexBuilder;
import com.baidu.hugegraph.loader.executor.LoadContext;
import com.baidu.hugegraph.loader.executor.LoadOptions;
import com.baidu.hugegraph.loader.mapping.EdgeMapping;
import com.baidu.hugegraph.loader.mapping.VertexMapping;
import com.baidu.hugegraph.loader.mapping.ElementMapping;
import com.baidu.hugegraph.loader.mapping.InputStruct;
import com.baidu.hugegraph.loader.mapping.LoadMapping;
import com.baidu.hugegraph.loader.source.InputSource;
import com.baidu.hugegraph.loader.source.file.FileFilter;
import com.baidu.hugegraph.loader.source.file.FileFormat;
import com.baidu.hugegraph.loader.source.file.FileSource;
import com.baidu.hugegraph.loader.source.file.SkippedLine;
import com.baidu.hugegraph.loader.source.file.Compression;
import com.baidu.hugegraph.loader.util.Printer;
import com.baidu.hugegraph.structure.GraphElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.graph.UpdateStrategy;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.BatchEdgeRequest;
import com.baidu.hugegraph.structure.graph.BatchVertexRequest;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class HugeGraphSparkLoaderHFile1 implements Serializable {

    public static final Logger LOG = Log.logger(HugeGraphSparkLoader.class);

    private final LoadOptions loadOptions;
    private final Map<ElementBuilder, List<GraphElement>> builders;
    //private final Map<String, List<BackendEntry>> buildersEntry;
    public static final String COL_FAMILY = "pd";
    private static final String APP_NAME = "FisherCoder-HFile-Generator";
//    private final HugeConfig config;
//    private final BinarySerializer hbaseSer ;


    public static void main(String[] args) throws Exception {
        HugeGraphSparkLoaderHFile1 loader;
        try {
            loader = new HugeGraphSparkLoaderHFile1(args);
        } catch (Throwable e) {
            Printer.printError("Failed to start loading", e);
            return;
        }
        loader.load();
    }

    public HugeGraphSparkLoaderHFile1(String[] args) {
        this.loadOptions = LoadOptions.parseOptions(args);
        this.builders = new HashMap<>();
        //this.buildersEntry = new HashMap<>();
//        this.config = com.baidu.hugegraph.unit.FakeObjects.newConfig();
//        this.config.setProperty("hbase.vertex_partitions",4);
//        this.config.setProperty("hbase.enable_partition",true);
//        this.config.setProperty("hbase.edge_partitions",4);
//        this.hbaseSer = new HbaseSerializer(config);
    }

    public void load() throws Exception {
        LoadMapping mapping = LoadMapping.of(this.loadOptions.file);
        List<InputStruct> structs = mapping.structs();

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
        for (InputStruct struct : structs) {
            if (false) {//API
                Dataset<Row> ds = read(spark, struct); //根据struct format 读取对应的source 文件
                ds.foreachPartition((Iterator<Row> p) -> {
                    LoadContext context = initPartition(this.loadOptions, struct);
                    p.forEachRemaining((Row row) -> {
                        loadRow(struct, row, p, context);
                    });
                    context.close();
                });
            } else if (true) {//HFile
                JavaRDD<Row> rdd = readJsonTable(spark);
                hFilesToHBase(rdd);
            } else if (false) {//sst

            }

        }
        spark.close();
        spark.stop();
    }

    private LoadContext initPartition(
            LoadOptions loadOptions, InputStruct struct) {
        LoadContext context = new LoadContext(loadOptions);
        for (VertexMapping vertexMapping : struct.vertices()) {
            this.builders.put(
                    new VertexBuilder(context, struct, vertexMapping),
                    new ArrayList<>());
        }
        for (EdgeMapping edgeMapping : struct.edges()) {
            this.builders.put(new EdgeBuilder(context, struct, edgeMapping),
                    new ArrayList<>());
        }
        return context;
    }

    private void loadRow(InputStruct struct, Row row, Iterator<Row> p,
                         LoadContext context) {
        for (Map.Entry<ElementBuilder, List<GraphElement>> builderMap :
                this.builders.entrySet()) {// 控制类型 点/边
            ElementMapping elementMapping = builderMap.getKey().mapping();
            // Parse
            if (elementMapping.skip()) {
                continue;
            }
            parse(row, builderMap, struct);

            // Insert
            List<GraphElement> graphElements = builderMap.getValue();
            if (graphElements.size() > elementMapping.batchSize() ||
                    (!p.hasNext() && graphElements.size() > 0)) {
                flush(builderMap, context.client().graph(),
                        this.loadOptions.checkVertex,"test");
            }
        }
    }

    private Dataset<Row> read(SparkSession ss, InputStruct struct) {
        InputSource input = struct.input();
        String charset = input.charset();
        FileSource fileSource = input.asFileSource();

        String[] header = fileSource.header();
        String delimiter = fileSource.delimiter();
        String path = fileSource.path();
        FileFilter filter = fileSource.filter();
        FileFormat format = fileSource.format();
        String dateFormat = fileSource.dateFormat();
        String timeZone = fileSource.timeZone();
        SkippedLine skippedLine = fileSource.skippedLine();
        Compression compression = fileSource.compression();
        int batchSize = fileSource.batchSize();

        DataFrameReader reader = ss.read();
        Dataset<Row> ds;
        switch (input.type()) {
            case FILE:
            case HDFS:
                switch (format) {
                    case TEXT:
                        ds = reader.text(path);
                        break;
                    case JSON:
                        ds = reader.json(path);
                        break;
                    case CSV:
                        ds = reader.csv(path);
                        break;
                    default:
                        throw new IllegalStateException(
                                "Unexpected format value: " + format);
                }
                break;
            case JDBC:
                // TODO: implement jdbc
            default:
                throw new AssertionError(String.format(
                        "Unsupported input source '%s'", input.type()));
        }
        return ds;
    }

    private void parse(Row row,
                       Map.Entry<ElementBuilder, List<GraphElement>> builderMap,
                       InputStruct struct) {
        ElementBuilder builder = builderMap.getKey();
        List<GraphElement> graphElements = builderMap.getValue();
        if ("".equals(row.mkString())) {
            return;
        }
        List<GraphElement> elements;
        switch (struct.input().type()) {
            case FILE:
            case HDFS:
                FileSource fileSource = struct.input().asFileSource();
                String [] headers =  fileSource.header();
                LOG.debug("Row: " + row.mkString());
                String [] values = row.mkString().split(fileSource.delimiter());
                LOG.debug("header length: " + headers.length);
                LOG.debug("Row Split length: " + values.length);
                for (int i = 0; i < fileSource.header().length; i++) {
                    LOG.debug("header: " + headers[i]);
                    LOG.debug("Row Split : " + values[i]);
                }



                LOG.debug("delimiter: " + fileSource.delimiter());
                elements = builder.build(fileSource.header(),
                        row.mkString()
                                .split(fileSource.delimiter()));//此处 只要传递的是对应的字符数组即可
                //TODO： 优化空间 ｜存在BUG 改为 "," 成功运行
                break;
            case JDBC:
                //TODO: implement jdbc
            default:
                throw new AssertionError(String.format(
                        "Unsupported input source '%s'",
                        struct.input().type()));
        }
        graphElements.addAll(elements);
    }

    private void flush(Map.Entry<ElementBuilder, List<GraphElement>> builderMap,
                       GraphManager g, boolean isCheckVertex, String sinkType) {
        ElementBuilder builder = builderMap.getKey();
        ElementMapping elementMapping = builder.mapping();
        List<GraphElement> graphElements = builderMap.getValue();
        boolean isVertex = builder.mapping().type().isVertex();
        List<BackendEntry> elements = new ArrayList<>();
        Map<String, UpdateStrategy> updateStrategyMap = elementMapping.updateStrategies();
        if (updateStrategyMap.isEmpty()) {
            if (isVertex) {
                for(HugeVertex vertex:(List< HugeVertex>) (Object) graphElements){
//                    BackendEntry entryVertex = hbaseSer.writeVertex(vertex);
//                    elements.add(entryVertex);
                }
                //this.buildersEntry.put("vertex",elements);
                //g.addVertices((List<Vertex>) (Object) graphElements);
            } else {
                //g.addEdges((List<Edge>) (Object) graphElements);
                for(HugeEdge edge:(List< HugeEdge>) (Object) graphElements){
//                    BackendEntry entryEdge = hbaseSer.writeEdge(edge);
//                    elements.add(entryEdge);
                }
                //this.buildersEntry.put("edge",elements);
            }
            elements.clear();//清理数据
        } else {
            // CreateIfNotExist dose not support false now
            if (isVertex) {
                BatchVertexRequest.Builder req =
                        new BatchVertexRequest.Builder();
                req.vertices((List<Vertex>) (Object) graphElements)
                        .updatingStrategies(updateStrategyMap)
                        .createIfNotExist(true);
                g.updateVertices(req.build());
            } else {
                BatchEdgeRequest.Builder req = new BatchEdgeRequest.Builder();
                req.edges((List<Edge>) (Object) graphElements)
                        .updatingStrategies(updateStrategyMap)
                        .checkVertex(isCheckVertex)
                        .createIfNotExist(true);
                g.updateEdges(req.build());
            }
        }
        graphElements.clear();
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
    //public void hFilesToHBase(JavaRDD<Row> rdd, InputStruct struct, BinarySerializer hbaseSer, Map<ElementBuilder, List<GraphElement>> builders,Map<String, List<BackendEntry>> buildersEntry) throws Exception {

    public void hFilesToHBase(JavaRDD<Row> rdd) throws Exception {
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {

                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        /**
                         * 核心逻辑：
                         * Row ---》BackendEntry
                         */
                        HugeConfig config = com.baidu.hugegraph.unit.FakeObjects.newConfig();
                        BinarySerializer hbaseSer = new HbaseSerializer(config);
                        config.setProperty("hbase.vertex_partitions",4);
                        config.setProperty("hbase.enable_partition",true);
                        config.setProperty("hbase.edge_partitions",4);

                        HugeEdge edge = new com.baidu.hugegraph.unit.FakeObjects().newEdge("123", "456");
                        BackendEntry entryEdge = hbaseSer.writeEdge(edge);
                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
                        KeyValue keyValue = null;
                        byte[] rowKeyBytes = entryEdge.id().asBytes();
                        rowKey.set(rowKeyBytes);

                        for(BackendEntry.BackendColumn column : entryEdge.columns()){
                            keyValue = new KeyValue(rowKeyBytes,
                                    Bytes.toBytes(COL_FAMILY),
                                    column.name,
                                    column.value);
                        }

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

        Partitioner partitioner = new HugeGraphSparkLoaderHFile1.IntPartitioner(5);//预分区处理

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


//    /**
//     * 核心步骤：
//     * 1.读取 source 文件 序列化
//     * 2.进行预分区
//     * 3.生成HFile
//     * 4.load 到HBase
//     * @param rdd
//     * @throws Exception
//     */
//    protected void hFilesToHBase(JavaRDD<Row> rdd) throws Exception {
//        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
//                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {
//                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
//                        String key = (String) row.get(0);
//                        String value = (String) row.get(1);
//
////                        HugeConfig config = com.baidu.hugegraph.unit.FakeObjects.newConfig();
////                        config.setProperty("hbase.vertex_partitions",4);
////                        config.setProperty("hbase.enable_partition",true);
////                        config.setProperty("hbase.edge_partitions",4);
////
////                        BinarySerializer hbaseSer = new HbaseSerializer(config);
//                        HugeEdge edge = new com.baidu.hugegraph.unit.FakeObjects().newEdge("123", "456");
//
//                        BackendEntry entryEdge = hbaseSer.writeEdge(edge);
//
//                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
//                        byte[] rowKeyBytes = entryEdge.id().asBytes();
//                        rowKey.set(rowKeyBytes);
//                        KeyValue keyValue = null;
//                        for(BackendEntry.BackendColumn column : entryEdge.columns()){
//                            keyValue = new KeyValue(rowKeyBytes,
//                                    Bytes.toBytes(COL_FAMILY),
//                                    column.name,
//                                    column.value);
//                        }
//
//                        return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, keyValue);
//                    }
//                });
//
//        Configuration baseConf = HBaseConfiguration.create();
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", "localhost");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        Connection conn = ConnectionFactory.createConnection(conf);
//        Table table = conn.getTable(TableName.valueOf("test_salt1"));
//
//        Job job = new Job(baseConf, APP_NAME);
//
//        Partitioner partitioner = new HugeGraphSparkLoaderHFile1.IntPartitioner(5);//预分区处理
//
//        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd =
//                javaPairRdd.repartitionAndSortWithinPartitions(partitioner);//精髓 partition内部做到有序
//
//
//        HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor());
//
//        System.out.println("Done configuring incremental load....");
//
//        Configuration config = job.getConfiguration();
//        FileSystem fs = FileSystem.get(config);
//        String path  = fs.getWorkingDirectory().toString()+"/hfile-gen"+"/"+System.currentTimeMillis();//HFile 存储路径
//
//        repartitionedRdd.saveAsNewAPIHadoopFile(
//                path,
//                ImmutableBytesWritable.class,
//                KeyValue.class,
//                HFileOutputFormat2.class,
//                config
//        );
//        System.out.println("Saved to HFiles to: " + path);
//
//        if (true) {
//            FsShell shell = new FsShell(conf);
//            try {
//                shell.run(new String[]{"-chmod", "-R", "777", path});
//            } catch (Exception e) {
//                System.out.println("Couldnt change the file permissions " + e
//                        + " Please run command:"
//                        + "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles "
//                        + path + " '"
//                        + "test" + "'\n"
//                        + " to load generated HFiles into HBase table");
//            }
//            System.out.println("Permissions changed.......");
//            loadHfiles(path);
//            System.out.println("HFiles are loaded into HBase tables....");
//        }
//    }

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

    public JavaRDD<Row> readJsonTable(SparkSession spark) throws ClassNotFoundException {

        Dataset<Row> rows = null;
        String path = "file:///Users/yangjiaqi/Documents/learnStudent/ETLWork/test.csv";
        rows = spark.read().csv(path);
        return rows.toJavaRDD();
    }
}

