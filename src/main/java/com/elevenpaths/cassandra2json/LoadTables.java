package com.elevenpaths.cassandra2json;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LoadTables {

    public static void main(String[] args) throws Exception {

        Cluster cluster = Cluster.builder().addContactPoint(args[0]).build();
        Session session = cluster.connect("telefonica_bi_mss");

        for (int i = 1; i < args.length; i++) {
            loadTable(session, args[i], args[i] + ".json");
        }

        session.close();
        cluster.close();

    }

    private static void loadTable(Session session, String table, String fileName) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        String line = null;
        BufferedReader in = new BufferedReader(new FileReader(fileName));

        int counter = 0;

        while ((line = in.readLine()) != null) {

            ObjectNode json = (ObjectNode)mapper.readTree(line);

            List<String> nullFieldNames = new ArrayList<String>();
            Iterator<String> iterator = json.fieldNames();

            while (iterator.hasNext()) {

                String fieldName = iterator.next();

                if (json.get(fieldName).isNull()) {
                    nullFieldNames.add(fieldName);
                }

            }

            for (String fieldName : nullFieldNames) {
                json.remove(fieldName);
            }

            List<String> fieldNames = new ArrayList<String>();
            List<Object> fieldValues = new ArrayList<Object>();

            iterator = json.fieldNames();

            while (iterator.hasNext()) {

                String fieldName = iterator.next();

                fieldNames.add(fieldName);

                if (json.get(fieldName).isBoolean()) {
                    fieldValues.add(json.get(fieldName).asBoolean());
                } else if (json.get(fieldName).isInt()) {
                    fieldValues.add(json.get(fieldName).asInt());
                } else if (json.get(fieldName).isLong()) {
                    fieldValues.add(json.get(fieldName).asLong());
                } else if (json.get(fieldName).isDouble()) {
                    fieldValues.add(json.get(fieldName).asDouble());
                } else if (json.get(fieldName).isTextual()) {
                    if (json.get(fieldName).asText().equals("NaN")) {
                        fieldValues.add(Double.NaN);
                    } else {
                        fieldValues.add(json.get(fieldName).asText());
                    }
                } else if (json.get(fieldName).isObject()) {

                    ObjectNode node = (ObjectNode)json.get(fieldName);

                    if (table.equalsIgnoreCase("mss_client_indicators")) {
                        if (fieldName.equalsIgnoreCase("umbral_compuesto")) {
                            Map<String,Double> fieldValue = new HashMap<String,Double>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_dwh_indicators")){
                        if (fieldName.equalsIgnoreCase("compound_threshold")) {
                            Map<String,String> fieldValue = new HashMap<String,String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("operador")) {
                            Map<String,String> fieldValue = new HashMap<String,String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_threshold_unit_type")){
                        if (fieldName.equalsIgnoreCase("units")) {
                            Map<Integer,String> fieldValue = new HashMap<Integer,String>();
                            Map<String,String> tmp = new HashMap<String,String>();
                            tmp = mapper.readValue(mapper.writeValueAsString(node), tmp.getClass());
                            for (Entry<String,String> entry : tmp.entrySet()) {
                                fieldValue.put(Integer.parseInt(entry.getKey()), entry.getValue());
                            }
                            fieldValues.add(fieldValue);
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else {
                        System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                        System.err.println(table);
                        System.err.println(json);
                    }

                } else if (json.get(fieldName).isArray()) {

                    ArrayNode node = (ArrayNode)json.get(fieldName);

                    if (table.equalsIgnoreCase("mss_dwh_concepts")) {
                        if (fieldName.equalsIgnoreCase("fields")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_dwh_indicators")){
                        if (fieldName.equalsIgnoreCase("eq")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("formula")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("gt")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("gte")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("ids_conceptos")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("is_not_null")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("is_null")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("like")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("lt")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("lte")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("neq")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("regex")) {
                            List<Integer> fieldValue = new ArrayList<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_dwh_ticket_problem_types")){
                        if (fieldName.equalsIgnoreCase("ticket_product_type_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("ticket_subcategory_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("ticket_type_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_dwh_ticket_product_types")){
                        if (fieldName.equalsIgnoreCase("ticket_subcategory_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("ticket_type_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_dwh_ticket_subcategories")){
                        if (fieldName.equalsIgnoreCase("ticket_type_ids")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_indicator_operations")){
                        if (fieldName.equalsIgnoreCase("fields")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("values")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_int_service_families")){
                        if (fieldName.equalsIgnoreCase("ids_servicios")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_int_services")){
                        if (fieldName.equalsIgnoreCase("ids_indicadores")) {
                            Set<Integer> fieldValue = new HashSet<Integer>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_security_alarm_data_indicators")){
                        if (fieldName.equalsIgnoreCase("event")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else if (table.equalsIgnoreCase("mss_ticket_data_indicators")){
                        if (fieldName.equalsIgnoreCase("alarm_category")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("alarm_extensions")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("alarm_extensions")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("categoria_producto")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("category")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("contexto_sensor")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("event")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("severity")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else if (fieldName.equalsIgnoreCase("src_range")) {
                            List<String> fieldValue = new ArrayList<String>();
                            fieldValues.add(mapper.readValue(mapper.writeValueAsString(node), fieldValue.getClass()));
                        } else {
                            System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                            System.err.println(table);
                            System.err.println(json);
                        }
                    } else {
                        System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                        System.err.println(table);
                        System.err.println(json);
                    }
                } else {
                    System.err.println("ALERT!" + json.get(fieldName).getNodeType().name());
                    System.err.println(table);
                    System.err.println(json);
                }

            }

            Statement statement = QueryBuilder.insertInto(table).values(fieldNames, fieldValues);

            try {
                session.execute(statement);
            } catch (Exception e) {
                System.out.println(statement.toString());
            }

            counter++;

            if (counter % 1000 == 0) {
                System.err.println("[" + table + "] Inserted " + counter + " entries...");
            }

        }

        in.close();

    }

}

