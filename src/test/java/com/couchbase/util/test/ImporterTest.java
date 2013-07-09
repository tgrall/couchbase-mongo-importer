package com.couchbase.util.test;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.util.MongoImporter;
import com.google.gson.Gson;
import com.mongodb.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import javax.swing.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ImporterTest {


    private static CouchbaseClient couchbaseClient = null;
    private static Mongo mongoClient = null;
    private static DB mongoDB = null;

    private static String dbName = "cbImpTest";

    private static String couchbaseCluster = "http://127.0.0.1:8091/pools";
    private static String bucket = "default";
    private static String password = "";
    private static String administrator = "Administrator";
    private static String adminPassword = "welcome1";
    private static List<URI> uris = new ArrayList<URI>();


    public ImporterTest() {

    }


    @BeforeClass
    public static void before() throws Exception {
        uris.add(new URI(couchbaseCluster));


        DBCollection employee = getMongoDB().getCollection("employee");
        // insert 100 document as employee
        for (int i = 1; i <= 100; i++) {
            BasicDBObject document = new BasicDBObject();
            document.put("_id", i);
            document.put("first", "John" + i);
            document.put("last", "Doe" + i);
            document.put("age", 30);
            document.put("createdDate", new Date());
            employee.insert(document, WriteConcern.SAFE);
        }

        DBCollection dept = getMongoDB().getCollection("dept");
        BasicDBObject document = new BasicDBObject();
        document.put("_id", "sales");
        document.put("name", "SALES");
        document.put("city", "London");
        document.put("createdDate", new Date());
        dept.insert(document, WriteConcern.SAFE);


    }

    @AfterClass
    public static void after() throws Exception {
        DBCollection employee = getMongoDB().getCollection("employee");
        employee.drop();
        DBCollection dept = getMongoDB().getCollection("dept");
        dept.drop();



        ClusterManager clusterManager = new ClusterManager(uris, administrator, adminPassword);
        clusterManager.flushBucket(bucket);
    }


    @Test
    public void importSingleCollectionTest() {
        MongoImporter importer = new MongoImporter();
        try {
            String[] args = new String[] {
                    "-n", couchbaseCluster,
                    "-b", bucket,
                    "-db", dbName,
                    "-c", "employee",
                    "-v"};
            MongoImporter.main( args );
            String employee = (String) getCouchbaseClient().get("1");
            Gson json = new Gson();
            HashMap<KeyStroke, Object> o = json.fromJson(employee, HashMap.class);
            assertEquals("John1", o.get("first"));
            assertEquals("Doe1", o.get("last"));
            assertEquals(30.0, o.get("age"));

            employee = (String) getCouchbaseClient().get("99");
            o = json.fromJson(employee, HashMap.class);
            assertEquals("John99", o.get("first"));
            assertEquals("Doe99", o.get("last"));
            assertEquals(30.0, o.get("age"));


        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void importAllCollections() {
        MongoImporter importer = new MongoImporter();
        try {
            String[] args = new String[] {
                    "-n", couchbaseCluster,
                    "-b", bucket,
                    "-db", dbName,
                    "-c", "employee",
                    "-v"};
            MongoImporter.main( args );

            Thread.sleep(3000);

//            String employee = (String) getCouchbaseClient().get("22");
//            Gson json = new Gson();
//            HashMap<KeyStroke, Object> o = json.fromJson(employee, HashMap.class);
//            assertEquals("John22", o.get("first"));
//            assertEquals("Doe22", o.get("last"));
//            assertEquals(30.0, o.get("age"));
//
//            employee = (String) getCouchbaseClient().get("33");
//            o = json.fromJson(employee, HashMap.class);
//            assertEquals("John33", o.get("first"));
//            assertEquals("Doe33", o.get("last"));
//            assertEquals(30.0, o.get("age"));


        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    private static DB getMongoDB() throws UnknownHostException {
        if (mongoDB == null) {
            mongoDB = getMongoClient().getDB(dbName);
        }
        return mongoDB;
    }

    private static Mongo getMongoClient() throws UnknownHostException {
        if (mongoClient == null) {
            mongoClient = new MongoClient("localhost", 27017);
        }
        return mongoClient;
    }

    private static CouchbaseClient getCouchbaseClient() {
        if (couchbaseClient == null) {
            // create Couchbase client
            try {
                couchbaseClient = new CouchbaseClient(uris, bucket, password);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return couchbaseClient;

    }

}
