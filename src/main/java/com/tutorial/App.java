package com.tutorial;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.*;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.io.File;
import java.util.Map;
import java.util.logging.Logger;
import java.io.IOException;

/**
 * Hello world!
 */
public class App {

    private static final String DEFAULT_BUCKET_NAME = "vengatem";
//    private static final Region DEFAULT_REGION = Region.getRegion(Regions.US_EAST_1);
    private static final Region DEFAULT_REGION = Region.getRegion(Regions.US_EAST_1);

    private AmazonS3 s3;
    private String bucketName;
    // Actual prefix to use
    private String prefix;
    // Actual region to use
    public Region region;
    private AWSCredentialsProvider credentialsProvider;
    private AmazonS3Client s3Client;



    public static void main(String[] args) {
        App app = new App();
/*        app.listS3Objects();
        File in = new File("test.docx");
        app.s3FileUpload(in);
        app.listS3Objects();
        app.s3FileDelete("test.docx");
        app.listS3Objects(); */
        InputStream input = App.class.getResourceAsStream("/test.docx");
        app.s3StreamUpload(input,"test.docx");
        app.listS3Objects();
        File test2 = new File("test2.docx");
        app.s3FileDownload("test.docx",test2);
        if(test2.exists()) {
            System.out.println("download successful");
        }

    }

    public App() {

        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AWSCredentials creds = credentialsProvider.getCredentials();

        bucketName = (System.getenv("bucket.name") == null) ? DEFAULT_BUCKET_NAME : System.getenv("bucket.name");
        region = (System.getenv("region") == null) ? DEFAULT_REGION
                : Region.getRegion(Regions.fromName(System.getenv("region")));
        System.out.println("Region is " + region);
        s3Client = new AmazonS3Client(credentialsProvider);

    }

    public void listS3Objects() {

        s3Client.setRegion(region);

        ObjectListing ol = s3Client.listObjects("vengatem");
        List<S3ObjectSummary> objSummary = ol.getObjectSummaries();
        for(S3ObjectSummary sos:objSummary) {
            System.out.println(sos.getKey() + sos.getSize());
            ObjectMetadata metadata = s3Client.getObjectMetadata(sos.getBucketName(),sos.getKey());
  //          System.out.println("Content type is " + metadata.getContentType());

            Map userMetaData = metadata.getUserMetadata();
            System.out.println("User metadata is " + userMetaData.toString());



        }

    }

    public void s3FileUpload(File fileName) {

        String toName = fileName.getName();
//        System.out.println("Uploading " + toName + "...");
        PutObjectResult result = s3Client.putObject(bucketName,toName,fileName);


    }

    public File s3FileDownload(String objectName,File file) {

        GetObjectRequest objRequest = new GetObjectRequest(bucketName,objectName);
        ObjectMetadata omd = s3Client.getObject(objRequest,file);
        return file;

    }

    public void s3FileDelete(String objectName) {
        System.out.println("Deleting " + objectName + "...");
        s3Client.deleteObject(bucketName,objectName);
    }

    public void s3StreamUpload(InputStream in, String toName) {

        ObjectMetadata omd = new ObjectMetadata();
        omd.addUserMetadata("Sample-metadata","tutorial");
//        System.out.println("Uploading " + toName + "...");
//        System.out.println("metadata is " + omd.getUserMetadata());
        PutObjectResult result = s3Client.putObject(bucketName,toName,in,omd);

    }
}
