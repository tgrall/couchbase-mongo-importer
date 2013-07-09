package com.couchbase.util;


import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.*;
import com.mongodb.*;
import org.apache.commons.cli.*;
import org.bson.types.ObjectId;

/**
 * Main example demonstrating Apache Commons CLI. Apache Commons CLI and more
 * details on it are available at http://commons.apache.org/cli/.
 *
 * @author Dustin
 */
public class MongoImporter {

    private final static String CMD_HELP = "h";
    private final static String CMD_CB_NODE = "n";
    private final static String CMD_CB_BUCKET = "b";
    private final static String CMD_CB_PASSWORD = "cp";

    private final static String CMD_MONGO_HOST = "h";
    private final static String CMD_MONGO_PORT = "p";
    private final static String CMD_MONGO_DB = "db";
    private final static String CMD_MONGO_USER = "mu";
    private final static String CMD_MONGO_PWD = "mp";
    private final static String CMD_MONGO_COLLS = "c";

    private final static String CMD_OUTPUT_DIR = "o";
    private final static String CMD_VERBOSE = "v";

    private final static String CMD_TYPE_FIELD = "t";
    private final static String CMD_KEY_PREFIX = "k";


    private static CouchbaseClient couchbaseClient = null;
    private static MongoClient mongo = null;
    private static DB mongoDB = null;

    // Couchbase information
    private static List uris = new ArrayList();
    private static String bucket = "default";
    private static String clusterURI = "http://127.0.0.1:8091/pools";
    private static String password = "";
    private static String docTypeName = null;
    private static boolean keyPrefix = false;


    // Mongo information
    private static String mongoHost = "127.0.0.1";
    private static String mongoPort = "27017";
    private static String mongoUser = null;
    private static String mongoPassword = null;
    private static String database = null;
    private static String collections = null;


    private static String outDirName = null;
    private static File outputDirectory = null;
    private static boolean verbose = false;


    private static Options options = new Options();
    private static Gson gson = null;


    /**
     * Executed command
     *
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) {

        MongoImporter importer = new MongoImporter();
        try {
            removeCouchbaseLogs();
            createOptions();
            parseOptions(args);
            if (verbose) {
                printAllParameters(System.out);
            }
            System.out.println("\n***** Importing ******");
            importer.importCollections();
        } catch (Exception e) {
            System.out.println("\n\n COUCHBASE : Mongo Importer");
            System.out.println("\n\n-------------------- ERROR --------------------");
            System.out.println("\t" + e.getMessage());
            System.out.println("\n\n\n");
            System.exit(-1);
        }
        importer.shutdown();
        System.out.println("\n\n******* End ********\n\n");

    }

    /**
     * Parse the different options using http://commons.apache.org/proper/commons-cli/
     * Set all options in local variables
     *
     * @param args
     * @throws Exception
     */
    private static void parseOptions(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        // show help if necessary
        if (cmd.hasOption(CMD_HELP)) {
            showHelp(options);
            System.exit(1);
        }
        verbose = cmd.hasOption(CMD_VERBOSE);
        clusterURI = cmd.getOptionValue(CMD_CB_NODE, clusterURI);
        bucket = cmd.getOptionValue(CMD_CB_BUCKET, bucket);
        password = cmd.getOptionValue(CMD_CB_PASSWORD, password);
        mongoHost = cmd.getOptionValue(CMD_MONGO_HOST, mongoHost);
        mongoPort = cmd.getOptionValue(CMD_MONGO_PORT, mongoPort);
        mongoUser = cmd.getOptionValue(CMD_MONGO_USER);
        mongoPassword = cmd.getOptionValue(CMD_MONGO_PWD);
        if (cmd.hasOption(CMD_MONGO_DB)) {
            database = cmd.getOptionValue(CMD_MONGO_DB);
        } else {
            throw new Exception("Database '" + CMD_MONGO_DB + "' is a mandatory parameter.");
        }

        docTypeName = cmd.getOptionValue(CMD_TYPE_FIELD);
        keyPrefix = cmd.hasOption(CMD_KEY_PREFIX);
        collections = cmd.getOptionValue(CMD_MONGO_COLLS);
        outDirName = cmd.getOptionValue(CMD_OUTPUT_DIR);

        createOutputDirIfNeeded();
    }

    private static void createOutputDirIfNeeded() {
        //Check if outputdir  exists if exists check directory
        if (outDirName != null) {
            outputDirectory = new File(outDirName);
            if (!outputDirectory.exists()) {
                boolean result = outputDirectory.mkdir();
                if (result) {
                    System.out.printf("\n'%s' created.\n", outDirName);
                }
            } else if (!outputDirectory.isDirectory()) {
                System.out.printf("'%s' is not a directory, so it cannot be used.\n", outputDirectory);
            }
        }
    }

    private static void removeCouchbaseLogs() {
        // remove log info from Couchbase
        Properties systemProperties = System.getProperties();
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        System.setProperties(systemProperties);
        Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.WARNING);
        for (Handler h : logger.getParent().getHandlers()) {
            if (h instanceof ConsoleHandler) h.setLevel(Level.WARNING);
        }
    }

    private static void printAllParameters(OutputStream out) {
        final PrintWriter writer = new PrintWriter(out);
        final HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printWrapped(writer, 80, "Couchbase Mongo Importer call with the following parameters:");
        usageFormatter.printWrapped(writer, 80, "\t From " + mongoHost + " " + mongoPort);
        usageFormatter.printWrapped(writer, 80, "\t Collections : " + ((collections == null) ? "ALL" : collections));
        if (outputDirectory != null) {
            usageFormatter.printWrapped(writer, 80, "\t To directory " + outputDirectory.getAbsolutePath());
        } else {
            usageFormatter.printWrapped(writer, 80, "\t To Couchbase " + clusterURI);
            usageFormatter.printWrapped(writer, 80, "\t bucket " + bucket);
        }
    }

    /**
     * Create the options
     */
    private static void createOptions() {
        options.addOption(CMD_CB_NODE, "node", true, "Couchbase Node")
                .addOption(CMD_CB_BUCKET, "bucket", true, "Couchbase Bucket")
                .addOption(CMD_CB_PASSWORD, "bucketPassword", true, "Couchbase Bucket Password")
                .addOption(CMD_TYPE_FIELD, "fieldtype", true, "If set, put the name of the collection as a 'type field' using this name")
                .addOption(CMD_KEY_PREFIX, "prefix", false, "if yes, it will prefix the key with the name of the collection")
                .addOption(CMD_MONGO_HOST, "host", true, "Mongodb Host")
                .addOption(CMD_MONGO_PORT, "port", true, "Mongodb Port")
                .addOption(CMD_MONGO_DB, "database", true, "Mongodb Database")
                .addOption(CMD_MONGO_USER, "mongoUser", true, "Mongodb Username")
                .addOption(CMD_MONGO_PWD, "mongoPassword", true, "Mongodb Password")
                .addOption(CMD_MONGO_COLLS, "collections", true, "Collections as comma separated list, if not specifield ALL collections will be imported")
                .addOption(CMD_OUTPUT_DIR, "output", true, "Output Directory, will put all JSON document separate file, to be used with cbdocloader")
                .addOption(CMD_VERBOSE, "verbose", false, "Be more verbose")
                .addOption(CMD_HELP, "help", false, "Help");
    }


    public static void importCollections() throws Exception {
        DB db = getMongoClient().getDB(database);
        Set<String> collList = null;
        if (collections == null || collections.isEmpty()) {
            collList = db.getCollectionNames();
        } else {
            collList = new HashSet<String>(Arrays.asList(collections.split(",")));
        }
        for (String coll : collList) {
            if (!coll.startsWith("system")) {
                importCollection(coll.trim());
            }
        }
    }


    public static void importCollection(String collection) throws Exception {
        System.out.printf("\n\nImporting collection %s\n", collection);
        DBCollection dbCollection = getMongoDB().getCollection(collection);
        DBCursor cursor = dbCollection.find();
        try {
            while (cursor.hasNext()) {
                DBObject object = cursor.next();
                String id = object.get("_id").toString();

                if (keyPrefix) {
                    id = collection + "::" + id;
                }

                if (docTypeName != null && !docTypeName.isEmpty()) {
                    object.put(docTypeName, collection);
                }
                String json = getJsonParser().toJson(object);
                if (outDirName != null) {
                    File jsonFile = new File(outputDirectory, id + ".json");
                    FileWriter writer = new FileWriter(jsonFile);
                    writer.write(json);
                    writer.close();
                } else {
                    getCouchbaseClient().set(id, json).get();
                }
            }
        } finally {
            cursor.close();
        }
    }


    public static CouchbaseClient getCouchbaseClient() throws Exception {
        if (couchbaseClient == null) {
            try {
            uris.add(new URI(clusterURI));
            couchbaseClient = new CouchbaseClient(uris, bucket, password);
            } catch (Exception e) {
                throw new Exception("Unable to connect to couchbase: \n\t"+ e.getMessage());
            }
        }
        return couchbaseClient;
    }


    public static MongoClient getMongoClient() throws UnknownHostException {
        if (mongo == null) {
            mongo = new MongoClient("localhost", 27017);
        }
        return mongo;
    }

    public static DB getMongoDB() throws Exception {
        if (mongoDB == null) {
            mongoDB = getMongoClient().getDB(database);
            if (mongoUser != null && !mongoUser.isEmpty()) {
                mongoDB.authenticate(mongoUser, mongoPassword.toCharArray());
            }
        }

        return mongoDB;
    }

    private static Gson getJsonParser() {
        if (gson == null) {
            JsonSerializer<ObjectId> serObjectId = new JsonSerializer<ObjectId>() {
                @Override
                public JsonElement serialize(ObjectId src, Type typeOfSrc, JsonSerializationContext
                        context) {
                    return src == null ? null : new JsonPrimitive(src.toString());
                }
            };
            JsonSerializer<Date> serDate = new JsonSerializer<Date>() {
                @Override
                public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext
                        context) {
                    return src == null ? null : new JsonPrimitive(src.getTime());
                }
            };
            gson = new GsonBuilder()
                    .registerTypeAdapter(ObjectId.class, serObjectId)
                    .registerTypeAdapter(Date.class, serDate).create();
        }
        return gson;
    }


    private static void shutdown() {
        if (couchbaseClient != null) {
            couchbaseClient.shutdown(10, TimeUnit.SECONDS);
        }
    }


    // static
    private static void showHelp(Options options) {
        HelpFormatter h = new HelpFormatter();
        h.printHelp("help", options);
    }

}

