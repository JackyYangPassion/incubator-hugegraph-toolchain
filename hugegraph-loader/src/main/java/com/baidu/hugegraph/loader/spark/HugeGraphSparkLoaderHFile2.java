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
import com.baidu.hugegraph.loader.direct.util.HBaseSerializer;
import com.baidu.hugegraph.loader.direct.util.SinkToHBase;
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


    private static HBaseSerializer serializer;
    private static SinkToHBase sinkToHBase;
    public static void main(String[] args) throws Exception {
        HugeGraphSparkLoaderHFile2 loader;
        try {
            loader = new HugeGraphSparkLoaderHFile2(args);
        } catch (Throwable e) {
            Printer.printError("Failed to start loading", e);
            return;
        }
        serializer = new HBaseSerializer();
        sinkToHBase = new SinkToHBase(serializer);
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
                sinkToHBase.hFilesToHBase(rdd, struct, this.loadOptions);
            } else if (false) {//sst
                // TODO : Gen-SST

            }
        }
        serializer.close();
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
                flush(builderMap, context.client().graph(), this.loadOptions.checkVertex);
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
                elements = builder.build(fileSource.header(), row.mkString().split(fileSource.delimiter()));//此处 只要传递的是对应的字符数组即可
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
                       GraphManager g, boolean isCheckVertex) {
        ElementBuilder builder = builderMap.getKey();
        ElementMapping elementMapping = builder.mapping();
        List<GraphElement> graphElements = builderMap.getValue();
        boolean isVertex = builder.mapping().type().isVertex();
        Map<String, UpdateStrategy> updateStrategyMap = elementMapping.updateStrategies();
        if (updateStrategyMap.isEmpty()) {
            if (isVertex) {
                g.addVertices((List<Vertex>) (Object) graphElements);
            } else {
                g.addEdges((List<Edge>) (Object) graphElements);
            }
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
}
