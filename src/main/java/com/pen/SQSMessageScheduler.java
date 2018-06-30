package com.pen;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class SQSMessageScheduler {
	
	
	
	static AmazonSQS sqs=null;
	static String myQueueUrl=null;
	
	static Map<Integer,Message> map=new HashMap();
	static Map<Integer,Message> map1=new HashMap();
	
	 public static void main(String[] args) throws Exception {
	
  
		 String propertiesFileName="D:\\Demos/SpringBootDemo/SqsTest/src/main/resources/application.properties";
		 
		 Properties properties = new Properties();
	     
		 properties.load(new FileInputStream(propertiesFileName));
	     //   return prop;
	            
	            String accessKey = properties.getProperty("AWSAccessKeyId");
	            String secretKey = properties.getProperty("AWSSecretKey");
	            AWSCredentials credentials = new BasicAWSCredentials(accessKey,
	                    secretKey);
	             sqs = new AmazonSQSClient(credentials);
	     
     //  	String myQueueUrl="https://sqs.ap-south-1.amazonaws.com/773068390205/queue1";
       	
       	 CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue4");
       	  myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
       	
       	System.out.println("-----first queue----------");
        
        Employe e=new Employe();
        e.setId(35);
        e.setName("puyyy");
        e.setSal("8484");
        
        ObjectMapper mapperObj = new ObjectMapper();
        String jsonStr=null;
        try {
             jsonStr = mapperObj.writeValueAsString(e);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
         
       // sqs.sendMessage(new SendMessageRequest(myQueueUrl, jsonStr));
     
       System.out.println("--------------");
       secondquee();
      
    }
	 
	 
	 public static void secondquee() throws IOException {
		 
	 	 CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue7");
      	 String  myQueueUrl11 = sqs.createQueue(createQueueRequest).getQueueUrl();
      	
      	System.out.println("-----second queue----------");
         
		 
		  boolean b=true;
	        while(b) {
	        
	        ReceiveMessageRequest rec=new ReceiveMessageRequest(myQueueUrl);
	        rec.setVisibilityTimeout(10);
	        rec.setMaxNumberOfMessages(10);
	        rec.withMaxNumberOfMessages(10);
	       
	        List<Message> messages = sqs.receiveMessage(rec).getMessages();
	        
	        if(messages.size() == 0) {
	        	b=false;
	        	break;
	        }
	        
	        
	        for (Message m : messages) {
	            
	        //	System.out.println(m.getBody()+"===========");
	        	String body = m.getBody();
	        	
	        	 ObjectMapper mapperObj = new ObjectMapper();
	             Employe emp=null;
	             try {
	                  emp = mapperObj.readValue(body,Employe.class);
	                  map.put(emp.getId(),m);
	                  
	             } catch (IOException e1) {
	                 // TODO Auto-generated catch block
	                 e1.printStackTrace();
	             }
	             
	             emp.setSal("222");
	             String jsonStr = mapperObj.writeValueAsString(emp);
	        	
	           //final String messageReceiptHandle = messages.get(0).getReceiptHandle();
	        	//sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, m.getReceiptHandle()));
	        	SendMessageResult mess = sqs.sendMessage(new SendMessageRequest(myQueueUrl11, jsonStr));
	        
	        	
	           }
	        
	        } 
	        thirdqueue();
		 
	 }
	 
	 public static void thirdqueue() throws IOException{
		 	System.out.println("-----third queue----------");
		      
	
		 CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue7");
      	 String  myQueueUrl11 = sqs.createQueue(createQueueRequest).getQueueUrl();
      	
         CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("MyQueue9");
     	 String  myQueueUrl13 = sqs.createQueue(createQueueRequest1).getQueueUrl();
     	
      	 
		 
		  boolean b=true;
	        while(b) {
	        
	        ReceiveMessageRequest rec=new ReceiveMessageRequest(myQueueUrl11);
	        rec.setVisibilityTimeout(10);
	        rec.setMaxNumberOfMessages(10);
	        rec.withMaxNumberOfMessages(10);
	       
	        List<Message> messages = sqs.receiveMessage(rec).getMessages();
	        
	        if(messages.size() == 0) {
	        	b=false;
	        	break;
	        }
	        
	        
	        for (Message m : messages) {
	            
	        	//System.out.println(m.getBody()+"=====endiddididid======");
	        	String body = m.getBody();
	        	
	        	 ObjectMapper mapperObj = new ObjectMapper();
	             Employe emp=null;
	             try {
	                  emp = mapperObj.readValue(body,Employe.class);
	                  map1.put(emp.getId(),m);
	                  
	             } catch (IOException e1) {
	                 // TODO Auto-generated catch block
	                 e1.printStackTrace();
	             }
	             
	            // emp.setSal("222");
	             String jsonStr = mapperObj.writeValueAsString(emp);
	        	
	           //final String messageReceiptHandle = messages.get(0).getReceiptHandle();
	        	//sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, m.getReceiptHandle()));
	        	SendMessageResult mess = sqs.sendMessage(new SendMessageRequest(myQueueUrl13, jsonStr));
	        
	        	
	           }
	        
	        } 
	
	        fourthqueue();	 
		 
		 
		 
		 
		 
	 }
	 
	 public static void fourthqueue() throws JsonParseException, JsonMappingException, IOException {
		 
		 	System.out.println("-----fourth queue----------");
		      
		 CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue7");
      	 String  myQueueUrl11 = sqs.createQueue(createQueueRequest).getQueueUrl();
     
		 
		 CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("MyQueue9");
     	 String  myQueueUrl13 = sqs.createQueue(createQueueRequest1).getQueueUrl();
    
     	 
     	  boolean b=true;
	        while(b) {
	        
	        ReceiveMessageRequest rec=new ReceiveMessageRequest(myQueueUrl13);
	        rec.setVisibilityTimeout(10);
	        rec.setMaxNumberOfMessages(10);
	        rec.withMaxNumberOfMessages(10);
	       
	        List<Message> messages = sqs.receiveMessage(rec).getMessages();
	        
	        if(messages.size() == 0) {
	        	b=false;
	        	break;
	        }
	   
	        System.out.println(map.size()+"======mapppppppppppp=====");
	        System.out.println(map1.size()+"=====mappppppppppp111111111======");
	   
	        
	        for (Message m : messages) {
	            
	        	System.out.println(m.getBody()+"=======endididididiggg====");
	        	String body = m.getBody();
	        	
	        	 ObjectMapper mapperObj = new ObjectMapper();
	             Employe emp=null;
	           
	                   emp = mapperObj.readValue(body,Employe.class);
	                  // map1.put(emp.getId(),m);
	                  
	                Message message22 = map.get(emp.getId());
	                Message message33 = map1.get(emp.getId());
	                if(message22 !=null) {
	                sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, message22.getReceiptHandle()));
	                System.out.println("=======kkkkk====");
	                }
	                if(message33 !=null) {
	                	   sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl11, message33.getReceiptHandle()));
	                }
	                 map.remove(emp.getId());
	    	         map1.remove(emp.getId());
	    	     
	    	         System.out.println("=======fff====");
	           }
	        System.out.println("=======gggg====");
	      //  sqs.deleteQueue(myQueueUrl13);
	        
	              
	        
	        } 
	
	        System.out.println(map.size()+"======mapppppppppppp=====");
	        System.out.println(map1.size()+"=====mappppppppppp111111111======");
	        boolean b1=true;
	        while(b1) {
	        ReceiveMessageRequest rec1=new ReceiveMessageRequest(myQueueUrl);
	        rec1.setVisibilityTimeout(10);
	        rec1.setMaxNumberOfMessages(10);
	        rec1.withMaxNumberOfMessages(10);
	       
	        List<Message> messages11 = sqs.receiveMessage(rec1).getMessages();
	        
	        if(messages11.size() == 0) {
	        	b1=false;
	        	break;
	        }
	        
	    	System.out.println("====before=======");
	        for (Message m : messages11) {
	            
	        	System.out.println(m.getBody()+"====afterrrrr=======");
	        	String body = m.getBody();
	        	
	        }
	        
	
	       }
	        
	        
	        boolean b3=true;
	        while(b3) {
	        ReceiveMessageRequest rec2=new ReceiveMessageRequest(myQueueUrl11);
	        rec2.setVisibilityTimeout(10);
	        rec2.setMaxNumberOfMessages(10);
	        rec2.withMaxNumberOfMessages(10);
	       
	        List<Message> messages11 = sqs.receiveMessage(rec2).getMessages();
	        
	        if(messages11.size() == 0) {
	        	b3=false;
	        	break;
	        }
	        
	    	System.out.println("====2nd-------before=======");
	        for (Message m : messages11) {
	            
	        	System.out.println(m.getBody()+"====afterrrrr=======");
	        	String body = m.getBody();
	        	
	        }
	        
	
	       }
	        
		 
	 }
	 
	 
	 
	 
}