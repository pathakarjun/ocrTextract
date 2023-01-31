package com.example;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.AmazonTextractException;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DetectDocumentTextRequest;
import com.amazonaws.services.textract.model.DetectDocumentTextResult;
import com.amazonaws.services.textract.model.Document;

public class Textract_inputFile {

    static AmazonTextractClientBuilder clientBuilder = AmazonTextractClientBuilder.standard()
            .withRegion("us-east-1");

    public static void main(String[] args) throws IOException {
        String sourceDoc = "C:\\Users\\arjpathak\\Desktop\\Dummy1.pdf";

        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("AKIA4GGYBAJNRDTV546Y",
                "xNQdw3l5vyMlaM3jFnPT03MaLIJ1lNi0Ll8yfGZ3")));
        AmazonTextract textractClient = clientBuilder.build();

        detectDocText(textractClient, sourceDoc);
    }

    public static void detectDocText(AmazonTextract textractClient, String sourceDoc) throws IOException {

        try {
            // InputStream sourceStream = new FileInputStream(new File(sourceDoc));
            // byte[] sourceBytes = IOUtils.toByteArray(sourceStream);
            Path sourceStream = Paths.get(sourceDoc);
            byte[] sourceBytes = Files.readAllBytes(sourceStream);

            // Get the input Document object as bytes
            Document myDoc = new Document()
                    .withBytes(ByteBuffer.wrap(sourceBytes));

            DetectDocumentTextRequest request = new DetectDocumentTextRequest()
                    .withDocument(myDoc);

            DetectDocumentTextResult result = textractClient.detectDocumentText(request);
            // System.out.println(result);

            for (Block block : result.getBlocks()) {
                System.out.println(block.toString());
            }

            // result.getBlocks().forEach(block -> {
            // if (block.getBlockType().equals("LINE"))

            // System.out.println("text is " + block.getText() + " confidence is " +
            // block.getConfidence());
            // });

        } catch (AmazonTextractException | FileNotFoundException e) {

            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}