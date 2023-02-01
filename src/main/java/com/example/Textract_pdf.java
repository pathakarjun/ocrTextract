package com.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.DocumentMetadata;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentAnalysisResult;
import com.amazonaws.services.textract.model.NotificationChannel;
import com.amazonaws.services.textract.model.Relationship;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.StartDocumentAnalysisResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;;

public class Textract_pdf {
    public static final String xmlFilePath = "C:\\Users\\arjpathak\\Desktop\\xmlOutput.xml";

    private static String sqsQueueName = null;
    private static String snsTopicName = null;
    private static String snsTopicArn = null;
    private static String roleArn = null;
    private static String sqsQueueUrl = null;
    private static String sqsQueueArn = null;
    private static String startJobId = null;
    private static String bucket = null;
    private static String document = null;
    private static AmazonSQS sqs = null;
    private static AmazonSNS sns = null;
    private static AmazonTextract textract = null;

    static AmazonTextractClientBuilder clientBuilder = AmazonTextractClientBuilder.standard()
            .withRegion(Regions.US_EAST_1);
    static AmazonSNSClientBuilder snsclientBuilder = AmazonSNSClientBuilder.standard()
            .withRegion(Regions.US_EAST_1);
    static AmazonSQSClientBuilder sqsclientBuilder = AmazonSQSClientBuilder.standard()
            .withRegion(Regions.US_EAST_1);

    public static void main(String[] args) throws Exception {
        // upload the document to s3
        // BasicAWSCredentials awsCreds = new
        // BasicAWSCredentials("AKIA4GGYBAJN6VPOI3FN",
        // "wmtp436NHH0Gt9+OAidokXEQscCry52ZzBiNwsFI");
        // AmazonS3 s3Client =
        // AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1)
        // .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        // .build();

        // String bucketName = "aspenbucket2";
        // String folderName = "folder1";
        // String fileNameInS3 = "out2.pdf";
        // String fileName = ".pdf";
        // String fileNameInLocalPC = "C:/Users/arjpathak/Desktop/" + fileName;
        // String s3BucketOwnerId =
        // s3Client.getBucketAcl(bucketName).getOwner().getId();

        // AccessControlList acl = new AccessControlList();
        // acl.grantPermission(new CanonicalGrantee(s3BucketOwnerId),
        // Permission.FullControl);
        // PutObjectRequest request = new PutObjectRequest(bucketName, folderName + "/"
        // + fileNameInS3,
        // new File(fileNameInLocalPC)).withAccessControlList(acl);
        // s3Client.putObject(request);
        // System.out.println("--Uploading file done");

        String document = "MED.pdf";
        String bucket = "aspenbucket2";
        String roleArn = "arn:aws:iam::837971673691:role/Textract-to-other-services-role";
        String accessKey = "AKIA4GGYBAJNRDTV546Y";
        String secretKey = "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";

        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKey, secretKey)));
        snsclientBuilder.setCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKey, secretKey)));
        sqsclientBuilder.setCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKey, secretKey)));

        sns = snsclientBuilder.build();
        sqs = sqsclientBuilder.build();
        textract = clientBuilder.build();

        CreateTopicandQueue();
        ProcessDocument(bucket, document, roleArn);
        DeleteTopicandQueue();
        System.out.println("Done!");
    }

    // Creates an SNS topic and SQS queue. The queue is subscribed to the topic.
    static void CreateTopicandQueue() {
        // create a new SNS topic
        snsTopicName = "AmazonTextractTopic" + Long.toString(System.currentTimeMillis());
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(snsTopicName);
        CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
        snsTopicArn = createTopicResult.getTopicArn();
        // Create a new SQS Queue
        sqsQueueName = "AmazonTextractQueue" + Long.toString(System.currentTimeMillis());
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest(sqsQueueName);
        sqsQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        sqsQueueArn = sqs.getQueueAttributes(sqsQueueUrl, Arrays.asList("QueueArn")).getAttributes().get("QueueArn");
        // Subscribe SQS queue to SNS topic
        String sqsSubscriptionArn = sns.subscribe(snsTopicArn, "sqs", sqsQueueArn).getSubscriptionArn();
        // Authorize queue
        Policy policy = new Policy().withStatements(
                new Statement(Effect.Allow)
                        .withPrincipals(Principal.AllUsers)
                        .withActions(SQSActions.SendMessage)
                        .withResources(new Resource(sqsQueueArn))
                        .withConditions(new Condition().withType("ArnEquals").withConditionKey("aws:SourceArn")
                                .withValues(snsTopicArn)));

        Map queueAttributes = new HashMap();
        queueAttributes.put(QueueAttributeName.Policy.toString(), policy.toJson());
        sqs.setQueueAttributes(new SetQueueAttributesRequest(sqsQueueUrl, queueAttributes));

        System.out.println("Topic arn: " + snsTopicArn);
        System.out.println("Queue arn: " + sqsQueueArn);
        System.out.println("Queue url: " + sqsQueueUrl);
        System.out.println("Queue sub arn: " + sqsSubscriptionArn);
    }

    static void DeleteTopicandQueue() {
        if (sqs != null) {
            sqs.deleteQueue(sqsQueueUrl);
            System.out.println("SQS queue deleted");
        }
        if (sns != null) {
            sns.deleteTopic(snsTopicArn);
            System.out.println("SNS topic deleted");
        }
    }

    // Starts the processing of the input document.
    static void ProcessDocument(String inBucket, String inDocument, String inRoleArn) throws Exception {
        bucket = inBucket;
        document = inDocument;
        roleArn = inRoleArn;
        StartDocumentAnalysis(bucket, document);
        System.out.println("Waiting for job: " + startJobId);
        // Poll queue for messages
        List<Message> messages = null;
        int dotLine = 0;
        boolean jobFound = false;
        // loop until the job status is published. Ignore other messages in queue.
        do {
            messages = sqs.receiveMessage(sqsQueueUrl).getMessages();
            if (dotLine++ < 40) {
                System.out.print(".");
            } else {
                System.out.println();
                dotLine = 0;
            }
            if (!messages.isEmpty()) {
                // Loop through messages received.
                for (Message message : messages) {
                    String notification = message.getBody();
                    // Get status and job id from notification.
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonMessageTree = mapper.readTree(notification);
                    JsonNode messageBodyText = jsonMessageTree.get("Message");
                    ObjectMapper operationResultMapper = new ObjectMapper();
                    JsonNode jsonResultTree = operationResultMapper.readTree(messageBodyText.textValue());
                    JsonNode operationJobId = jsonResultTree.get("JobId");
                    JsonNode operationStatus = jsonResultTree.get("Status");
                    System.out.println("Job found was " + operationJobId);
                    // Found job. Get the results and display.
                    if (operationJobId.asText().equals(startJobId)) {
                        jobFound = true;
                        System.out.println("Job id: " + operationJobId);
                        System.out.println("Status : " + operationStatus.toString());
                        if (operationStatus.asText().equals("SUCCEEDED")) {
                            GetDocumentAnalysisResults();
                        } else {
                            System.out.println("Document analysis failed");
                        }
                        sqs.deleteMessage(sqsQueueUrl, message.getReceiptHandle());
                    } else {
                        System.out.println("Job received was not job " +
                                startJobId);

                        // Delete unknown message. Consider moving message to dead letter queue.
                        sqs.deleteMessage(sqsQueueUrl, message.getReceiptHandle());
                    }
                }
            }
        } while (!jobFound);
        System.out.println("Finished processing document");
    }

    private static void StartDocumentAnalysis(String bucket, String document) throws Exception {
        // Create notification channel
        NotificationChannel channel = new NotificationChannel()
                .withSNSTopicArn(snsTopicArn)
                .withRoleArn(roleArn);
        StartDocumentAnalysisRequest req = new StartDocumentAnalysisRequest()
                .withFeatureTypes("TABLES", "FORMS")
                .withDocumentLocation(new DocumentLocation()
                        .withS3Object(new S3Object()
                                .withBucket(bucket)
                                .withName(document)))
                .withJobTag("AnalyzingText")
                .withNotificationChannel(channel);
        StartDocumentAnalysisResult startDocumentAnalysisResult = textract.startDocumentAnalysis(req);
        startJobId = startDocumentAnalysisResult.getJobId();
    }

    // Gets the results of processing started by StartDocumentAnalysis
    private static void GetDocumentAnalysisResults() throws FileNotFoundException {
        String paginationToken = null;
        GetDocumentAnalysisResult response = null;
        Boolean finished = false;
        Map<String, Block> blockMap = new LinkedHashMap<>();
        Map<String, Block> keyMap = new LinkedHashMap<>();
        Map<String, Block> valueMap = new LinkedHashMap<>();
        Map<String, Block> tableMap = new LinkedHashMap<>();
        Map<String, Block> cellMap = new LinkedHashMap<>();
        int totalPages = 0;

        // loops until pagination token is null
        while (finished == false) {
            GetDocumentAnalysisRequest documentAnalysisRequest = new GetDocumentAnalysisRequest()
                    .withJobId(startJobId)
                    .withNextToken(paginationToken);

            response = textract.getDocumentAnalysis(documentAnalysisRequest);

            // Show blocks, confidence and detection times
            List<Block> blocks = response.getBlocks();
            totalPages = response.getDocumentMetadata().getPages();

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
                if (b.getBlockType().equals("TABLE")) {
                    tableMap.put(block_id, b);
                }
                if (b.getBlockType().equals("CELL")) {
                    cellMap.put(block_id, b);
                }
            }
            paginationToken = response.getNextToken();
            if (paginationToken == null)
                finished = true;
        }
        getResult(blockMap, keyMap, valueMap, tableMap, cellMap, totalPages);
    }

    private static void getResult(Map<String, Block> blockMap, Map<String, Block> keyMap, Map<String, Block> valueMap,
            Map<String, Block> tableMap, Map<String, Block> cellMap, int totalPages) {
        try {
            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document = documentBuilder.newDocument();

            // root element
            Element root = document.createElement("document");
            document.appendChild(root);

            for (int pageNo = 1; pageNo <= totalPages; pageNo++) {
                // page element
                Element page = document.createElement("page");
                root.appendChild(page);
                // set an attribute to page element
                Attr attr = document.createAttribute("number");
                attr.setValue(String.valueOf(pageNo));
                page.setAttributeNode(attr);

                for (Map.Entry<String, Block> itr : tableMap.entrySet()) {
                    if (itr.getValue().getPage() == pageNo) {
                        int rowNo = 0;
                        Element table = document.createElement("table");
                        page.appendChild(table);
                        Attr id = document.createAttribute("id");
                        id.setValue(itr.getKey());
                        table.setAttributeNode(id);

                        Element row = null;
                        for (Relationship relationship : itr.getValue().getRelationships()) {
                            if (relationship.getType().toString().equals("CHILD")) {
                                for (String cellId : relationship.getIds()) {
                                    Block cBlock = cellMap.get(cellId);
                                    String text = getText(cBlock, blockMap);
                                    if (cBlock.getRowIndex() != rowNo) {
                                        row = document.createElement("row");
                                        table.appendChild(row);
                                        Attr rowIndex = document.createAttribute("rowIndex");
                                        rowIndex.setValue(String.valueOf(rowNo + 1));
                                        row.setAttributeNode(rowIndex);
                                        rowNo = cBlock.getRowIndex();
                                    }
                                    String col = (cBlock.getEntityTypes() != null
                                            && cBlock.getEntityTypes().toString().equals(
                                                    "[COLUMN_HEADER]")) ? "columnHeader"
                                                            : "column";
                                    Element column = document.createElement(col);
                                    column.appendChild(document.createTextNode(text));
                                    row.appendChild(column);
                                    Attr columnIndex = document.createAttribute("columnIndex");
                                    columnIndex.setValue(String.valueOf(cBlock.getColumnIndex()));
                                    column.setAttributeNode(columnIndex);
                                }
                            }
                        }
                    }
                }

                for (Map.Entry<String, Block> itr : keyMap.entrySet()) {
                    if (itr.getValue().getPage() == pageNo) {
                        Block valueBlock = findValue(itr.getValue(), valueMap);
                        String key = getText(itr.getValue(), blockMap);
                        String value = getText(valueBlock, blockMap);

                        Element keyTag = document.createElement("key");
                        keyTag.appendChild(document.createTextNode(value));
                        page.appendChild(keyTag);
                        Attr text = document.createAttribute("text");
                        text.setValue(key);
                        keyTag.setAttributeNode(text);
                    }
                }
            }
            // create the xml file
            // transform the DOM Object to an XML File
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File(xmlFilePath));
            // If you use
            // StreamResult result = new StreamResult(System.out);
            // the output will be pushed to the standard output ...
            // You can use that for debugging
            transformer.transform(domSource, streamResult);
            System.out.println("Done creating XML File " + "on location " + xmlFilePath);
        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
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
                        if (b != null && b.getBlockType().equals("SELECTION_ELEMENT")) {
                            stringBuilder.append(b.getSelectionStatus()).append(" ");
                        }
                    }
                }
            }
        }

        return stringBuilder.toString();
    }

    // private static void writeUsingFiles(String data, boolean format, String
    // fileName) throws FileNotFoundException {
    // String output = "C:/Users/arjpathak/Desktop/" + fileName;
    // File f = new File(output);

    // PrintWriter out = null;
    // if (f.exists() && !f.isDirectory()) {
    // out = new PrintWriter(new FileOutputStream(new File(output), true));
    // } else {
    // out = new PrintWriter(output);
    // }
    // if (format) {
    // out.println(data);
    // } else {
    // out.print(data);
    // }

    // out.close();
    // }
}