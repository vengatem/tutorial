package com.tutorial;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        App app = new App();
        app.process();

    }

    public void process() {
        DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();
        AmazonS3Client s3 = new AmazonS3Client(chain);
        ObjectListing ol = s3.listObjects("vengatem");
        List<S3ObjectSummary> objSummary = ol.getObjectSummaries();
        for(S3ObjectSummary sos:objSummary) {
            System.out.println(sos.getKey());
        }



    }
}
