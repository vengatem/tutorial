package com.tutorial;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.internal.PageIterable;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.elasticache.model.SourceType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Hello world!
 */
public class DynamoApp {

    private static final String DEFAULT_TABLE_NAME = "tutorial";
//    private static final Region DEFAULT_REGION = Region.getRegion(Regions.US_EAST_1);
    private static final Region DEFAULT_REGION = Region.getRegion(Regions.US_EAST_1);

    private DynamoDB dDB;
    private String tableName;
    private Table tutorialTable;

    // Actual region to use
    public Region region;
    private AWSCredentialsProvider credentialsProvider;



    public static void main(String[] args) {
        DynamoApp app = new DynamoApp();
//        app.listTable();
//        app.createTweet("a@b.com","Test1");
//        app.createTweet("b@c.com","Test2");
        System.out.println(app.getTweetAsMap("a@b.com",1457564841595L));

    }

    public DynamoApp() {

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        tableName = (System.getenv("table.name") == null) ? DEFAULT_TABLE_NAME : System.getenv("table.name");
        region = (System.getenv("region") == null) ? DEFAULT_REGION
                : Region.getRegion(Regions.fromName(System.getenv("region")));

        AmazonDynamoDB temp = new AmazonDynamoDBClient(credentialsProvider);
        temp.setRegion(region);
        dDB = new DynamoDB(temp);
        System.out.println("About to get table...");

        tutorialTable = dDB.getTable(tableName);


    }

    public void listTable() {

        TableCollection<ListTablesResult> tblCollection = dDB.listTables();

        PageIterable<Table, ListTablesResult> pages = tblCollection.pages();
        pages.forEach(new Consumer<Page<Table, ListTablesResult>>() {
            public void accept(Page<Table, ListTablesResult> tables) {
                tables.forEach(new Consumer<Table>() {
                    public void accept(Table table) {
                        System.out.println(table.getTableName());
                    }
                });
            }
        });

    }

    public void createTweet(String email, String text) {
        Objects.requireNonNull(email);
        Objects.requireNonNull(text);

        Item item = new Item().withPrimaryKey("email",email,"timestamp",System.currentTimeMillis()).withString("text",text);
        tutorialTable.putItem(item);

    }

    public Map<String,Object> getTweetAsMap(String email, long timeStamp) {
        Item item = tutorialTable.getItem("email",email,"timestamp",timeStamp);
        if(item == null) return null;
        else {
            return(item.asMap());
        }
    }

    public List<Map<String,Object>> getAllUserTweets(String email)
    {
        return null; // TODO: Implement me!
    }

}
