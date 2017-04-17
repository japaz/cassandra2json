package com.elevenpaths.cassandra2json;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DumpTables {

    public static void main(String[] args) throws Exception {

        Cluster cluster = Cluster.builder().addContactPoint(args[0]).build();
        Session session = cluster.connect("telefonica_bi_mss");

        for (int i = 2; i < args.length; i++) {

            Statement statement = QueryBuilder.select().from(args[i]);
            statement.setFetchSize(Integer.parseInt(args[1]));

            dumpTable(session, args[i] + ".json", statement);

        }

        session.close();
        cluster.close();

    }

    private static void dumpTable(Session session, String fileName, Statement statement) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        PrintWriter out = new PrintWriter(new FileWriter(fileName));

        ResultSet resultSet = session.execute(statement);

        int counter = 0;

        try {

            for (Row row : resultSet) {

                if (resultSet.getAvailableWithoutFetching() == 1000 && !resultSet.isFullyFetched()) {
                    resultSet.fetchMoreResults();
                }

                counter++;

                if (counter % 1000 == 0) {
                    System.err.println("Processed " + counter + " entries...");
                }

                ObjectNode json = JsonNodeFactory.instance.objectNode();

                for (ColumnDefinitions.Definition column : row.getColumnDefinitions()) {
                    json.putPOJO(column.getName(), row.getObject(column.getName()));
                }

                out.println(mapper.writeValueAsString(json));

            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        out.close();

    }

}