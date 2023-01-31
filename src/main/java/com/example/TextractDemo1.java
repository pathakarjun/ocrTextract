package com.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.AnalyzeDocumentRequest;
import com.amazonaws.services.textract.model.AnalyzeDocumentResult;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.Document;
import com.amazonaws.services.textract.model.HumanLoopConfig;
import com.amazonaws.services.textract.model.Relationship;
import com.amazonaws.services.textract.model.S3Object;

public class TextractDemo1 {

        static AmazonTextractClientBuilder clientBuilder = AmazonTextractClientBuilder.standard()
                        .withRegion(Regions.US_EAST_1);

        public static void main(String[] args) throws IOException {

                clientBuilder.setCredentials(
                                new AWSStaticCredentialsProvider(new BasicAWSCredentials("AKIA4GGYBAJNRDTV546Y",
                                                "xNQdw3l5vyMlaM3jFnPT03MaLIJ1lNi0Ll8yfGZ3")));

                String document = "walmart.png";
                String bucket = "aspenbucket2";

                AmazonTextract client = clientBuilder.build();
                // DetectDocumentTextRequest request = new DetectDocumentTextRequest()
                // .withDocument(new Document()
                // .withS3Object(new S3Object()
                // .withName(document)
                // .withBucket(bucket)));
                // DetectDocumentTextResult result = client.detectDocumentText(request);
                // System.out.println(result);

                // result.getBlocks().forEach(block -> {
                // if (block.getBlockType().equals("LINE"))

                // System.out.println("text is " + block.getText() + " confidence is "
                // + block.getConfidence());
                // });

                AnalyzeDocumentRequest request = new AnalyzeDocumentRequest()
                                .withFeatureTypes("TABLES", "FORMS")
                                .withHumanLoopConfig(new HumanLoopConfig().withHumanLoopName("walmartreviewpng")
                                                .withFlowDefinitionArn(
                                                                "arn:aws:sagemaker:us-east-1:837971673691:flow-definition/initialhumanworkflow"))
                                .withDocument(new Document()
                                                .withS3Object(new S3Object().withName(document).withBucket(bucket)));

                AnalyzeDocumentResult result = client.analyzeDocument(request);
                System.out.println(result.getHumanLoopActivationOutput());

                List<Block> blocks = result.getBlocks();

                Map<String, Block> blockMap = new LinkedHashMap<>();
                Map<String, Block> keyMap = new LinkedHashMap<>();
                Map<String, Block> valueMap = new LinkedHashMap<>();

                for (Block b : blocks) {
                        String block_id = b.getId();
                        blockMap.put(block_id, b);
                        if (b.getBlockType().equals("KEY_VALUE_SET")) {
                                for (String entityType : b.getEntityTypes()) {
                                        if (entityType.toString().equals("KEY")) {
                                                keyMap.put(block_id, b);
                                        } else {
                                                valueMap.put(block_id, b);
                                        }
                                }
                        }
                }

                result.getBlocks().forEach(block -> {
                        if (block.getBlockType().equals("LINE"))
                                try {
                                        writeUsingFiles(block.getText() + "   : Confidence - " +
                                                        block.getConfidence());
                                } catch (FileNotFoundException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }
                });

                writeUsingFiles(getRelationships(blockMap, keyMap, valueMap).toString());

        }

        public static Map<String, String> getRelationships(Map<String, Block> blockMap, Map<String, Block> keyMap,
                        Map<String, Block> valueMap) {
                Map<String, String> result = new LinkedHashMap<>();
                for (Map.Entry<String, Block> itr : keyMap.entrySet()) {
                        Block valueBlock = findValue(itr.getValue(), valueMap);
                        String key = getText(itr.getValue(), blockMap);
                        String value = getText(valueBlock, blockMap);
                        result.put(key, value);
                }
                return result;
        }

        public static Block findValue(Block keyBlock, Map<String, Block> valueMap) {
                Block b = null;
                for (Relationship relationship : keyBlock.getRelationships()) {
                        if (relationship.getType().toString().equals("VALUE")) {
                                for (String id : relationship.getIds()) {
                                        b = valueMap.get(id);
                                }
                        }
                }
                return b;
        }

        public static String getText(Block result, Map<String, Block> blockMap) {
                StringBuilder stringBuilder = new StringBuilder();
                if (result.getRelationships() != null) {
                        for (Relationship relationship : result.getRelationships()) {
                                if (relationship.getType().toString().equals("CHILD")) {
                                        for (String id : relationship.getIds()) {
                                                Block b = blockMap.get(id);
                                                if (b != null && b.getBlockType().equals("WORD")) {
                                                        stringBuilder.append(b.getText()).append(" ");
                                                }
                                        }
                                }
                        }
                }
                return stringBuilder.toString();
        }

        private static void writeUsingFiles(String data) throws FileNotFoundException {
                String output = "C:/Users/arjpathak/Desktop/output.txt";
                File f = new File(output);

                PrintWriter out = null;
                if (f.exists() && !f.isDirectory()) {
                        out = new PrintWriter(new FileOutputStream(new File(output), true));
                } else {
                        out = new PrintWriter(output);
                }
                out.println(data);
                out.close();
        }
}
