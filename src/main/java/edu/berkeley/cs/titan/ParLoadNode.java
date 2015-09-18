package edu.berkeley.cs;

import com.google.common.base.Splitter;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class ParLoadNode {

    static String nodeFile = null, edgeFile = null;

    public static void main(String[] args) throws ConfigurationException, IOException {
        Configuration config = new PropertiesConfiguration(
            Load.class.getResource("/benchmark.properties"));
        Configuration titanConfiguration = new PropertiesConfiguration(
            Load.class.getResource("/titan-cassandra.properties"));

        if (args.length > 0) {
            nodeFile = args[0];
        }
        if (args.length > 1) {
            edgeFile = args[1];
        }

        load(config, titanConfiguration);
    }

    public static void load(Configuration config, Configuration titanConfig) throws IOException {
        titanConfig.setProperty("storage.batch-loading", true);
        titanConfig.setProperty("schema.default", "none");
        titanConfig.setProperty("graph.set-vertex-id", true);
        titanConfig.setProperty("storage.cassandra.keyspace", config.getString("name"));

        TitanGraph g = TitanFactory.open(titanConfig);
        createSchemaIfNotExists(g, config);
        if (g.getVertices().iterator().hasNext()) {
            System.err.print("Warning! Graph already has data!");
        } else {
            loadGraph(g, config);
        }
    }

    private static void createSchemaIfNotExists(TitanGraph g, Configuration config) {
        TitanManagement mgmt = g.getManagementSystem();
        if (mgmt.containsEdgeLabel("0"))
            return;

        int NUM_ATTR = config.getInt("property.total");
        int NUM_ATYPES = config.getInt("atype.total");

        PropertyKey[] nodeProperties = new PropertyKey[NUM_ATTR];
        for (int i = 0; i < NUM_ATTR; i++) {
            nodeProperties[i] = mgmt.makePropertyKey("attr" + i).dataType(String.class).make();
            mgmt.buildIndex("byAttr" + i, Vertex.class).addKey(nodeProperties[i]).buildCompositeIndex();
        }

        PropertyKey timestamp = mgmt.makePropertyKey("timestamp").dataType(Long.class).make();
        PropertyKey edgeProperty = mgmt.makePropertyKey("property").dataType(String.class).make();
        for (int i = 0; i < NUM_ATYPES; i++) {
            EdgeLabel label = mgmt.makeEdgeLabel("" + i).signature(timestamp, edgeProperty).unidirected().make();
            if (config.getBoolean("index_timestamp")) {
                mgmt.buildEdgeIndex(label, "byEdge" + i, Direction.OUT, Order.DESC, timestamp);
            }
        }

        mgmt.commit();
    }

    private static void loadGraph(TitanGraph g, Configuration conf) throws IOException {
        BatchGraph bg = new BatchGraph(g, VertexIDType.NUMBER, 500000);

        int propertySize = conf.getInt("property.size");
        int numProperty = conf.getInt("property.total");
        String nodeFile = conf.getString("data.node");
        String edgeFile = conf.getString("data.edge");
        if (ParLoadNode.nodeFile != null) {
            nodeFile = ParLoadNode.nodeFile;
        }
        if (ParLoadNode.edgeFile != null) {
            edgeFile = ParLoadNode.edgeFile;
        }

        int offset = conf.getBoolean("zero_indexed") ? 1 : 0;
        System.out.printf("nodeFile %s, edgeFile %s, propertySize %d\n", nodeFile, edgeFile, propertySize);

        String[] properties = new String[numProperty * 2];
        for (int i = 0; i < numProperty * 2; i += 2) {
            properties[i] = "attr" + (i / 2);
        }

        // Expects input:
        // <node id> \x02 <prop1> \x02 ... <prop40> \x02

        long c = 1L;
        final int bufferSize = 8 * 1024 * 100; // roughly 1000 nodes at a time
        Splitter nodeTableSplitter = Splitter.on('\02');

        try (BufferedReader br = new BufferedReader(
            new FileReader(nodeFile), bufferSize)) {

            for (String line; (line = br.readLine()) != null; ) {
                Iterator<String> tokens = nodeTableSplitter
                    .split(line).iterator();
                long nodeId = Long.parseLong(tokens.next());
                for (int i = 0; i < numProperty; i++) {
                    // trim first delimiter character
                    properties[i * 2 + 1] = tokens.next();
                }
                // Hopefully reduces RPC trips.
                bg.addVertex(TitanId.toVertexId(nodeId), properties);
                if (++c % 100000L == 0L) {
                    System.out.println("Processed " + c + " nodes");
                }
            }
        }

        bg.commit();
    }
}
