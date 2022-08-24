package com.baidu.hugegraph.loader.spark;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.loader.builder.EdgeBuilder;
import com.baidu.hugegraph.loader.builder.ElementBuilder;
import com.baidu.hugegraph.loader.builder.VertexBuilder;
import com.baidu.hugegraph.loader.direct.struct.HugeType;
import com.baidu.hugegraph.loader.direct.util.BytesBuffer;
import com.baidu.hugegraph.loader.direct.util.GraphSchema;
import com.baidu.hugegraph.loader.executor.LoadContext;
import com.baidu.hugegraph.loader.executor.LoadOptions;
import com.baidu.hugegraph.loader.mapping.*;
import com.baidu.hugegraph.loader.source.InputSource;
import com.baidu.hugegraph.loader.source.file.*;
import com.baidu.hugegraph.loader.util.Printer;
import com.baidu.hugegraph.structure.GraphElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.graph.*;
import com.baidu.hugegraph.structure.schema.PropertyKey;
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

public class HugeGraphSparkLoaderHFile2 implements Serializable {

    public static final Logger LOG = Log.logger(HugeGraphSparkLoader.class);

    private transient final LoadOptions loadOptions;
    private transient final Map<ElementBuilder, List<GraphElement>> builders;

    public static final String COL_FAMILY = "pd";
    private static final String APP_NAME = "FisherCoder-HFile-Generator";
    private static final int edgeLogicPartitions = 16;
    private static final int vertexLogicPartitions = 8;
    private GraphSchema graphSchema;

    public static void main(String[] args) throws Exception {
        HugeGraphSparkLoaderHFile2 loader;
        try {
            loader = new HugeGraphSparkLoaderHFile2(args);
        } catch (Throwable e) {
            Printer.printError("Failed to start loading", e);
            return;
        }
        loader.load();
    }

    public HugeGraphSparkLoaderHFile2(String[] args) {
        this.loadOptions = LoadOptions.parseOptions(args);
        this.builders = new HashMap<>();

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

        HugeClient client = HugeClient.builder("http://localhost:8081", "hugegraph").build();
        graphSchema = new GraphSchema(client);//schema 缓存到内存 对象中 共享

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
                JavaRDD<Row> rdd = read(spark, struct).toJavaRDD();
                //JavaRDD<Row> rdd = readJsonTable(spark);
                LoadContext context = initPartition(this.loadOptions, struct);
                hFilesToHBase(rdd, struct, this.loadOptions);
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
    public void hFilesToHBase(JavaRDD<Row> rdd, InputStruct struct, LoadOptions loadOptions) throws Exception {
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {

                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        /**
                         * 核心逻辑：
                         * Row ---> Vertex/Edge ----> KV
                         */
                        List<GraphElement> elementsElement;//存储返回的  GraphElement: Vertex/Edge

                        Map<ElementBuilder, List<GraphElement>> builders1 = new HashMap<>();

                        LoadContext context = new LoadContext(loadOptions);
                        for (VertexMapping vertexMapping : struct.vertices()) {
                            builders1.put(
                                    new VertexBuilder(context, struct, vertexMapping),
                                    new ArrayList<>());
                        }
                        for (EdgeMapping edgeMapping : struct.edges()) {
                            builders1.put(new EdgeBuilder(context, struct, edgeMapping),
                                    new ArrayList<>());
                        }

                        /**
                         * 构建 vertex/edge --->  K-V
                         */
                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
                        KeyValue keyValue = null;

                        for (Map.Entry<ElementBuilder, List<GraphElement>> builderMap : builders1.entrySet()) {// 控制类型 点/边
                            ElementMapping elementMapping = builderMap.getKey().mapping();
                            // Parse
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

                                    elementsElement = builder.build(fileSource.header(),
                                            row.mkString()
                                                    .split(fileSource.delimiter()));//此处 只要传递的是对应的字符数组即可
                                    break;
                                default:
                                    throw new AssertionError(String.format(
                                            "Unsupported input source '%s'",
                                            struct.input().type()));
                            }

                            boolean isVertex = builder.mapping().type().isVertex();
                            if (isVertex) {
                                for (Vertex vertex : (List<Vertex>) (Object) elementsElement) {
                                    byte[] rowkey = getKeyBytes(vertex);
                                    byte[] values = getValueBytes(vertex);
                                    rowKey.set(rowkey);
                                    keyValue = new KeyValue(rowkey,
                                            Bytes.toBytes(COL_FAMILY),
                                            Bytes.toBytes(""),
                                            values);
                                }

                            } else {
                                for (Edge edge : (List<Edge>) (Object) elementsElement) {
                                    byte[] rowkey = getKeyBytes(edge);
                                    byte[] values = getValueBytes(edge);
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

        Configuration baseConf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("test_salt1"));

        Job job = new Job(baseConf, APP_NAME);

        Partitioner partitioner = new HugeGraphSparkLoaderHFile2.IntPartitioner(5);//预分区处理

        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd =
                javaPairRdd.repartitionAndSortWithinPartitions(partitioner);//精髓 partition内部做到有序


        HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor());

        System.out.println("Done configuring incremental load....");

        Configuration config = job.getConfiguration();
        FileSystem fs = FileSystem.get(config);
        String path = fs.getWorkingDirectory().toString() + "/hfile-gen" + "/" + System.currentTimeMillis();//HFile 存储路径

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



    public byte[] getKeyBytes(GraphElement e) {
        byte[] array = null;
        if(e.type() == "vertex" && e.id() != null){
            BytesBuffer buffer = BytesBuffer.allocate(2 + 1 + e.id().toString().length());
            buffer.writeShort(getPartition(HugeType.VERTEX, IdGenerator.of(e.id())));
            buffer.writeId(IdGenerator.of(e.id()));
            array = buffer.bytes();
        }else if ( e.type() == "edge" ){
            BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
            Edge edge = (Edge)e;
            buffer.writeShort(getPartition(HugeType.EDGE, IdGenerator.of(edge.sourceId())));
            buffer.writeId(IdGenerator.of(edge.sourceId()));
            buffer.write(HugeType.EDGE_OUT.code());
            long label = graphSchema.getEdgeLabel(e.label()).id();
            buffer.writeId(IdGenerator.of(graphSchema.getEdgeLabel(e.label()).id()));//出现错误
            buffer.writeStringWithEnding("");
            buffer.writeId(IdGenerator.of(edge.targetId()));
            array = buffer.bytes();
        }
        return array;
    }

    public short getPartition(HugeType type, Id id) {
        int hashcode = Arrays.hashCode(id.asBytes());
        short partition = 1;
        if (type.isEdge()) {
            partition = (short) (hashcode % edgeLogicPartitions);
        } else if (type.isVertex()) {
            partition = (short) (hashcode % vertexLogicPartitions);
        }
        return partition > 0 ? partition : (short) -partition;
    }


    public byte[] getValueBytes(GraphElement e) {
        byte[] array = null;
        if(e.type() == "vertex"){
            int propsCount = e.properties().size() ;//vertex.sizeOfProperties();
            BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);
            buffer.writeId(IdGenerator.of(graphSchema.getVertexLabel(e.label()).id()));
            buffer.writeVInt(propsCount);
            for(Map.Entry<String, Object> entry : e.properties().entrySet()){
                PropertyKey propertyKey = graphSchema.getPropertyKey(entry.getKey());
                buffer.writeVInt(propertyKey.id().intValue());
                buffer.writeProperty(propertyKey.dataType(),entry.getValue());
            }
            array = buffer.bytes();
        } else if ( e.type() == "edge" ){
            int propsCount =  e.properties().size();
            BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);
            buffer.writeVInt(propsCount);
            for(Map.Entry<String, Object> entry : e.properties().entrySet()){
                PropertyKey propertyKey = graphSchema.getPropertyKey(entry.getKey());
                buffer.writeVInt(propertyKey.id().intValue());
                buffer.writeProperty(propertyKey.dataType(),entry.getValue());
            }
            array = buffer.bytes();
        }

        return array;
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
}
