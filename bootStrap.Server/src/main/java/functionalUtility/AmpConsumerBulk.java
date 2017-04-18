/*package functionalUtility;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

//import java.awt.dnd.DnDConstants;
//import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
//import net.minidev.json.parser.JSONParser;
//import net.minidev.json.parser.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utility.commonUtility;
import com.utility.dbConnection;
import joptsimple.internal.Strings;

public class AmpConsumerBulk{

	static KafkaConsumer<String, String> consumer = null;
	static Map<Integer, Integer> hm = null;
	static dbConnection dbCon = null;
	static String ccwTable = null;
	static String ccwCommitTable = null;
	static Set<String> uniDataBatch = new HashSet<String>();
	static List<String> dupDataBatch = new ArrayList<String>();
	static Set<String> uniDataBatchMain = new HashSet<String>();
	static List<String> dupDataBatchMain = new ArrayList<String>();
	//New Var as per procedure
	static String[] setAPPL_NAME = null;
	static long[] setAppl_ID = null;
	static String[] setTRANSACTION_TYPE = null;
	static String[] setSUB_TRX_TYPE= null;
	static String[] setCCO_USER_ID = null;
	static long[] setINSTANCE_ID = null;
	static long[] setCONTRACT_NUMBER = null;
	static long[] setSERVICE_LINE_ID = null;
	static String[] setSOURCE_CP_LINE_ID = null;
	static String[] setTERMINATION_DATE =null;
	static long[] setOFFSET = null;
	static long[] setPARTITION = null;
	
	
	static int batchSize = 0;
	static int Poll_ms = 0;
	static String BackupTable = null;
	static String DupErrDataTable = null;
	static int size = 0;
	static int counter =0;
	static String mainTableColsList = null;	
	static String dupErrTableColsList = null;
	static int initFlag = 0;
	public static void main(String[] argv) {

		commonUtility comm = new commonUtility();
		Properties prop = comm.readProp();

		// fetching properties data as input
		// ======================================
		String topicName = prop.getProperty("AmpTopicname");
		String groupId = prop.getProperty("AmpGroupId");
		ccwTable = prop.getProperty("AmpApptable");
		ccwCommitTable = prop.getProperty("AmpCommitTable");
		String strServerName = prop.getProperty("AmpBootstrapServer");
		batchSize = Integer.parseInt(prop.getProperty("BatchSize").toString());
		Poll_ms = Integer.parseInt(prop.getProperty("Poll_Ms").toString());
		DupErrDataTable = prop.getProperty("AMPDupErrDataTable");
		mainTableColsList = prop.getProperty("AmpAppTableColsList");
		dupErrTableColsList = prop.getProperty("AMPDupErrDataTableColsList");
		//boolean initFlag = false;
		// Printing Confing file Data
		System.out.println("================================================");
		System.out.println("topic Name : " + topicName);
		System.out.println("group Id : " + groupId);
		System.out.println("Server : " + strServerName);
		//System.out.println("Table : " + ccwTable);
		System.out.println("Commit Main Table : " + ccwTable);
		System.out.println("Commit Offset table : " + ccwCommitTable);
		System.out.println("Dup/Error JSON Table : " + DupErrDataTable);
		System.out.println("Batch Size : " + batchSize);
		System.out.println("================================================");
		
		//Initialize Array

		setAPPL_NAME = new String[batchSize];
		setAppl_ID = new long[batchSize];
		setTRANSACTION_TYPE =  new String[batchSize];
		setSUB_TRX_TYPE=  new String[batchSize];
		setCCO_USER_ID =  new String[batchSize];
		setINSTANCE_ID =  new long[batchSize];
		setCONTRACT_NUMBER =  new long[batchSize];
		setSERVICE_LINE_ID =  new long[batchSize];
		setSOURCE_CP_LINE_ID =  new String[batchSize];
		setTERMINATION_DATE =  new String[batchSize];
		setOFFSET =  new long[batchSize];
		setPARTITION =  new long[batchSize];
		
		//
		
		// Kafka consumer configuration settings
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, strServerName);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer = new KafkaConsumer<String, String>(configProperties);

		// Fetching records from stored updated offset in Offcommit table
		dbCon = new dbConnection();
		String Query = " select * from " + ccwCommitTable;
		hm = dbCon.executeQuery(Query);
		if(hm.size() != 0){
			System.out.println("hm size is : "+ hm.size());
			initFlag = 1;
		}
		// print the topic name
		System.out.println("Subscribed to topic " + topicName);
		ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
				false);
		objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

		int i = 0;
		try {

			// Code for Seek operation
			consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					System.out.printf("%s topic-partitions are revoked from this consumer\n",
							Arrays.toString(partitions.toArray()));
				}

				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					System.out.printf("%s topic-partitions are assigned to this consumer\n",
							Arrays.toString(partitions.toArray()));
					Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
					while (topicPartitionIterator.hasNext()) {

						@SuppressWarnings("unused")
						String strCheck = null;
						TopicPartition topicPartition = topicPartitionIterator.next();

						System.out.println("==================================================================");
						System.out.println("Current offset is : " + consumer.position(topicPartition) + ", Partition : "
								+ topicPartition.partition() + ", committed offset is : "
								+ consumer.committed(topicPartition));
						System.out.println("topicPartition Name : " + topicPartition);
						System.out.println("==================================================================");

						// Code to check the new Partion_number
						boolean cFlag = true;
						for (@SuppressWarnings("rawtypes")
						Map.Entry e : hm.entrySet()) {
							if (Integer.parseInt(e.getKey().toString()) == topicPartition.partition()) {
								cFlag = false;
								break;
							}
						}

						if (cFlag) {
							System.out.println("New Partion Number is found : " + topicPartition.partition());
							try {
								hm.put(topicPartition.partition(), (int) consumer.position(topicPartition));
								dbCon.insertCommitedOffset(topicPartition.partition(),
										consumer.position(topicPartition), ccwCommitTable);
							} catch (SQLException e1) {
								System.out.println("Exception while inserting newly Partion Number in table");
								e1.printStackTrace();
							}
						}

						// Code to check the Offset seek position
						if (hm.get(topicPartition.partition()) == -2) {
							System.out.println("Leaving it alone");
						} else if (hm.get(topicPartition.partition()) == 0) {
							System.out.println("Setting offset to begining");

							try {
								dbCon.updateBeginningOffset(consumer.position(topicPartition),
										topicPartition.partition(), ccwCommitTable);
							} catch (SQLException e) {
								System.err.println("Exception in update beginning offset when Offset is 0");
								e.printStackTrace();
							}
							
							consumer.seekToBeginning(partitions);

						} else if (hm.get(topicPartition.partition()) == -1) {
							System.out.println("Setting it to the end ");
							consumer.seekToEnd(partitions);
						} else {

							// Seeking data as per partition and offset
							long currOffset = consumer.position(topicPartition);
							long commitOffset = Integer
									.parseInt((consumer.committed(topicPartition).toString()).replaceAll("[^0-9]", ""));

							System.out.println(topicPartition.partition() + " =>  Partition : "
									+ topicPartition.partition() + ", Offset : " + hm.get(topicPartition.partition()));

							if (hm.get(topicPartition.partition()) == 0) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from Current offset");
								 // consumer.seekToBeginning(partitions);
								consumer.seek(topicPartition, 0); // uncomment after testing
							} else if (hm.get(topicPartition.partition()) != 0) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from table commit offset");
								
								if(initFlag == 0){
									initFlag = 1;
									//System.out.println("Print 1");
									consumer.seekToBeginning(partitions);
								}else{
									 //consumer.seek(topicPartition,hm.get(topicPartition.partition()) + 1); // 449999
									//System.out.println("Print 2");
									 consumer.seek(topicPartition,1); // remove
								}
								 
							} else if ((!Strings
									.isNullOrEmpty(strCheck = consumer.committed(topicPartition).toString()))) {
								System.out
										.println(topicPartition.partition() + " Partition, Reading from Commit offset");
								consumer.seekToBeginning(partitions);  // remove
								//consumer.seek(topicPartition, commitOffset + 1);
							} else
								System.out.println("No Offset Matched.");

						}
					}
				}
			});

			// Loop to Poll data from partion
			boolean insertFlag = false;
			boolean recCheck = false;
			int pollCount = 0;
			dbConnection getconn = new dbConnection();
			while (true) {				

				ConsumerRecords<String, String> records = consumer.poll(Poll_ms);
				recCheck = false;
				// Partion processing
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

					// Processing records from individual partition
					for (ConsumerRecord<String, String> record : partitionRecords) {
						recCheck = true;						
						String JsongetString = new String(record.value());
						
						int intPartition_Number = record.partition();
						Integer intOffset = (int) record.offset();

						AmpCounsumerRespBean responseObj = null;
						try {

							objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);							
							responseObj = objectMapper.readValue(JsongetString, AmpCounsumerRespBean.class);
							System.out.println("JSON :" + JsongetString);
							uniDataBatch.add(JsongetString);
						} catch (JsonParseException e) {
							System.out.println("1. Json Parse Exception exception, hence inserting into error table");
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
								continue;
							}							
						} catch (JsonMappingException e) {
							System.out.println("2. JSON Mapping Exception Exception , hence error data inserting into error table ");
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
							}
							continue;							
						} catch (IOException e) {							
							System.out.println("3. IO Exception Exception , hence error data  inserting into error table");
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
							}
							continue;
						}

						// Code to store the latest offset and its partition
						hm.put(intPartition_Number, intOffset);

						if (size == uniDataBatch.size()){
							prepareDupDataBatchQuery(comm, responseObj, JsongetString, intOffset, intPartition_Number,
									DupErrDataTable, dupErrTableColsList);
							System.out.println("1. DupErrDataTable : "+ DupErrDataTable);
						}						
						else{
							prepareBatchQuery(comm, responseObj, intOffset, intPartition_Number, ccwTable,
									mainTableColsList, counter);
							counter++;
						}						
						size = uniDataBatch.size();

						if (i % 5 == 0) {
							long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
							//consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));							
						}

						// Creating batch as per config prop file
						if (++i % batchSize == 0) {
							if (uniDataBatch.size() > 0 || dupDataBatchMain.size() > 0) {							
								
								if (uniDataBatch.size() > 0) {
									System.out.println("1. ========== Insert into Unique table ===========");
									//dbCon.executeUniqueBatch(uniDataBatchMain, uniDataBatch, DupErrDataTable , dupErrTableColsList);
									
									System.out.println("----- setAPPL_NAME length : "+ setAPPL_NAME.length);
									dbCon.executeBatchProcedure( String[] setAPPL_NAME ,											
														//setAppl_ID,
														setTRANSACTION_TYPE ,
														 setSUB_TRX_TYPE ,
														 setCCO_USER_ID ,
														setINSTANCE_ID ,
														 setCONTRACT_NUMBER ,
														 setSERVICE_LINE_ID ,
														 setSOURCE_CP_LINE_ID ,
														 setTERMINATION_DATE ,
														 setOFFSET ,
														 setPARTITION
														);
									getconn.updateCommitedOffset(hm, ccwCommitTable);
								}
								if (dupDataBatchMain.size() > 0) {
									System.out.println("1. =========Insert into Duplicate table =============");
									dbCon.executeBatchWithDuplicateData(dupDataBatchMain);
								}
								insertFlag = true;
							}

							// Reset Values
							insertFlag = true;
							uniDataBatch.clear();
							uniDataBatchMain.clear();
							dupDataBatchMain.clear();
							size = 0;
							counter = 0;
							
						}
						
												
					} // End For loop of Partiton records 			

					//Commit Offset after partiton records iteration
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					//consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}

				// Commit offset after for Poll Loop
				if (recCheck) {					
					getconn.updateNewOffset(hm, ccwCommitTable);
				}

				if (uniDataBatch.size() > 0 || dupDataBatchMain.size() > 0) {

					if (uniDataBatch.size() > 0) {
						System.out.println("2. ========== Insert into Unique table =======");
						dbCon.executeUniqueBatch(uniDataBatchMain, uniDataBatch , DupErrDataTable , dupErrTableColsList);
						getconn.updateCommitedOffset(hm, ccwCommitTable);
					}
					if (dupDataBatchMain.size() > 0) {
						System.out.println("2. ========= Insert into Duplicate table =========");
						dbCon.executeBatchWithDuplicateData(dupDataBatchMain);
					}
					insertFlag = true;
					
					// Clear fields					
					uniDataBatch.clear();
					uniDataBatchMain.clear();
					dupDataBatchMain.clear();
					size = 0;
					counter = 0;
					System.out.println("Insert flag : " + insertFlag + ", Unique batch Size : " + uniDataBatch.size()
							+ ", Duplicate batch Size : " + dupDataBatch.size());
					
				}		
				insertFlag = false;			
				
			} // end While
		} catch (Exception e) {
			System.out.println("Exception in Consumer class : " + e.getMessage());
			String errMsg = e.getMessage().toString();
			if(errMsg.contains("poll()")){
				System.out.println("True Contain");
				AmpConsumerBulk.main(argv);
			}else{
				System.out.println("Please check the other Ex exception");
			}
		} finally {
			consumer.close();
		}

	}
	
	public static void prepareBatchQuery(commonUtility commObj, AmpCounsumerRespBean obj, Integer offset,
			int Partition_Number, String Table, String colsList, int counter) {
		String query = null;
		try {
			System.out.println("********* Counter : "+ counter);
			setAPPL_NAME[counter] = obj.getApplName();
			setAppl_ID[counter] = (int) obj.getAppReqId();	
			setTRANSACTION_TYPE[counter]  = obj.getTrnxType();
			setSUB_TRX_TYPE[counter]  = obj.getSubTnxType();
			setCCO_USER_ID[counter]  = obj.getCCOUserId() ;
			setINSTANCE_ID[counter]  = (int) obj.getInstanceId();
			setCONTRACT_NUMBER[counter]  = (int) obj.getContractNumber(); 	
			setSERVICE_LINE_ID[counter]  =  (int) obj.getSerLineId();	
			setSOURCE_CP_LINE_ID[counter]  =obj.getSrcCpLineId(); 	
			setTERMINATION_DATE[counter]  = obj.getTerDate() ;
			setOFFSET[counter]  = offset ;
			setPARTITION[counter]  = Partition_Number ;	
			
			System.out.println("setAPPL_NAME[counter] Count => "+ setAPPL_NAME.length);
			
		} catch (Exception e) {
			System.out.println("Exception in prepareBatchQuery : " + e.getMessage());
		}
	}

	public static void prepareDupDataBatchQuery(commonUtility commObj, AmpCounsumerRespBean obj, String jsonData,
			Integer offset, int Partition_Number, String Table, String colsList) {
		String query = null;
		try {
			query = "insert into " + Table + " (" + colsList + ")" + " values (" + obj.getAppReqId() + ",'" + jsonData
					+ "', sysdate )";
			if (!Strings.isNullOrEmpty(query)) {
				//System.out.println("Duplicate Query  : " + query);
				dupDataBatchMain.add(query);
			}
		} catch (Exception e) {
			System.out.println("Exception in prepareBatchQuery : " + e.getMessage());
		}
	}

}
*/







//-----------------------------------------




package functionalUtility;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

//import java.awt.dnd.DnDConstants;
//import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
//import net.minidev.json.parser.JSONParser;
//import net.minidev.json.parser.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utility.commonUtility;
import com.utility.dbConnection;
import joptsimple.internal.Strings;

public class AmpConsumerBulk {

	static KafkaConsumer<String, String> consumer = null;
	static Map<Integer, Integer> hm = null;
	static dbConnection dbCon = null;
	static String ccwTable = null;
	static String ccwCommitTable = null;
	static Set<String> uniDataBatch = new HashSet<String>();
	static List<String> dupDataBatch = new ArrayList<String>();
	static Set<String> uniDataBatchMain = new HashSet<String>();
	static List<String> dupDataBatchMain = new ArrayList<String>();
	static int batchSize = 0;
	static int Poll_ms = 0;
	static String BackupTable = null;
	static String DupErrDataTable = null;
	static int size = 0;
	static String mainTableColsList = null;	
	static String dupErrTableColsList = null;
	static int initFlag = 0;
	static String topicName = "";
	static String groupId = ""; 
	public static void main(String[] argv) {

		commonUtility comm = new commonUtility();
		Properties prop = comm.readProp();

		// fetching properties data as input
		// ======================================
		topicName = prop.getProperty("AmpTopicname");
		groupId = prop.getProperty("AmpGroupId");
		ccwTable = prop.getProperty("AmpApptable");
		ccwCommitTable = prop.getProperty("AmpCommitTable");
		String strServerName = prop.getProperty("AmpBootstrapServer");
		batchSize = Integer.parseInt(prop.getProperty("BatchSize").toString());
		Poll_ms = Integer.parseInt(prop.getProperty("Poll_Ms").toString());
		DupErrDataTable = prop.getProperty("AMPDupErrDataTable");
		mainTableColsList = prop.getProperty("AmpAppTableColsList");
		dupErrTableColsList = prop.getProperty("AMPDupErrDataTableColsList");
		//boolean initFlag = false;
		// Printing Confing file Data
		System.out.println("================================================");
		System.out.println("topic Name : " + topicName);
		System.out.println("group Id : " + groupId);
		System.out.println("Server : " + strServerName);
		//System.out.println("Table : " + ccwTable);
		System.out.println("Commit Main Table : " + ccwTable);
		System.out.println("Commit Offset table : " + ccwCommitTable);
		System.out.println("Dup/Error JSON Table : " + DupErrDataTable);
		System.out.println("Batch Size : " + batchSize);
		System.out.println("================================================");

		// Kafka consumer configuration settings
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, strServerName);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer = new KafkaConsumer<String, String>(configProperties);

		// Fetching records from stored updated offset in Offcommit table
		dbCon = new dbConnection();
		String Query = " select * from " + ccwCommitTable;
		hm = dbCon.executeQuery(Query);
		if(hm.size() != 0){
			System.out.println("hm size is : "+ hm.size());
			initFlag = 1;
		}
		// print the topic name
		System.out.println("Subscribed to topic " + topicName);
		ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
				false);
		objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

		int i = 0;
		try {

			// Code for Seek operation
			consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					System.out.printf("%s topic-partitions are revoked from this consumer\n",
							Arrays.toString(partitions.toArray()));
				}

				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					System.out.printf("%s topic-partitions are assigned to this consumer\n",
							Arrays.toString(partitions.toArray()));
					Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
					while (topicPartitionIterator.hasNext()) {

						@SuppressWarnings("unused")
						String strCheck = null;
						TopicPartition topicPartition = topicPartitionIterator.next();

						System.out.println("==================================================================");
						System.out.println("Current offset is : " + consumer.position(topicPartition) + ", Partition : "
								+ topicPartition.partition() + ", committed offset is : "
								+ consumer.committed(topicPartition));
						System.out.println("topicPartition Name : " + topicPartition);
						System.out.println("==================================================================");

						// Code to check the new Partion_number
						boolean cFlag = true;
						for (@SuppressWarnings("rawtypes")
						Map.Entry e : hm.entrySet()) {
							if (Integer.parseInt(e.getKey().toString()) == topicPartition.partition()) {
								cFlag = false;
								break;
							}
						}

						if (cFlag) {
							System.out.println("New Partion Number is found : " + topicPartition.partition());
							try {
								hm.put(topicPartition.partition(), (int) consumer.position(topicPartition));
								dbCon.insertCommitedOffset(topicPartition.partition(),
										consumer.position(topicPartition), ccwCommitTable,topicName,groupId);
								//1123
							} catch (SQLException e1) {
								System.out.println("Exception while inserting newly Partion Number in table");
								e1.printStackTrace();
							}
						}

						// Code to check the Offset seek position
						if (hm.get(topicPartition.partition()) == -2) {
							System.out.println("Leaving it alone");
						} else if (hm.get(topicPartition.partition()) == 0) {
							System.out.println("Setting offset to begining");

							try {
								dbCon.updateBeginningOffset(consumer.position(topicPartition),
										topicPartition.partition(), ccwCommitTable);
							} catch (SQLException e) {
								System.err.println("Exception in update beginning offset when Offset is 0");
								e.printStackTrace();
							}
							
							consumer.seekToBeginning(partitions);

						} else if (hm.get(topicPartition.partition()) == -1) {
							System.out.println("Setting it to the end ");
							consumer.seekToEnd(partitions);
						} else {

							// Seeking data as per partition and offset
							long currOffset = consumer.position(topicPartition);
							long commitOffset = Integer
									.parseInt((consumer.committed(topicPartition).toString()).replaceAll("[^0-9]", ""));

							System.out.println(topicPartition.partition() + " =>  Partition : "
									+ topicPartition.partition() + ", Offset : " + hm.get(topicPartition.partition()));

							if (hm.get(topicPartition.partition()) == 0) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from Current offset");
								  consumer.seekToBeginning(partitions);
								//consumer.seek(topicPartition, 0); // uncomment after testing
							} else if (hm.get(topicPartition.partition()) != 0) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from table commit offset");
								
								if(initFlag == 0){
									initFlag = 1;
									System.out.println("Print 1");
									consumer.seekToBeginning(partitions);
								}else{
									 consumer.seek(topicPartition,hm.get(topicPartition.partition()) + 1); // 449999
									System.out.println("Print 2");
									 //consumer.seek(topicPartition,130018);
								}
								 
							} else if ((!Strings
									.isNullOrEmpty(strCheck = consumer.committed(topicPartition).toString()))) {
								System.out
										.println(topicPartition.partition() + " Partition, Reading from Commit offset");
								//consumer.seekToBeginning(partitions);
								consumer.seek(topicPartition, commitOffset + 1);
							} else
								System.out.println("No Offset Matched.");

						}
					}
				}
			});

			// Loop to Poll data from partion
			boolean insertFlag = false;
			boolean recCheck = false;
			int pollCount = 0;
			dbConnection getconn = new dbConnection();
			while (true) {				
				
				ConsumerRecords<String, String> records = consumer.poll(Poll_ms);
				recCheck = false;
				// Partion processing
				//System.out.println("before for");
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

					// Processing records from individual partition
					for (ConsumerRecord<String, String> record : partitionRecords) {
						recCheck = true;						
						String JsongetString = new String(record.value());
						
						int intPartition_Number = record.partition();
						Integer intOffset = (int) record.offset();

						AmpCounsumerRespBean responseObj = null;
						try {
							System.out.println("in try=======1");
							objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);		
							System.out.println("in try=======2");
							responseObj = objectMapper.readValue(JsongetString, AmpCounsumerRespBean.class);
					        System.out.println("printing json string"+JsongetString );
							uniDataBatch.add(JsongetString);
						} catch (JsonParseException e) {
							System.out.println("1. Json Parse Exception exception, hence inserting into error table");
							e.printStackTrace();
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
								continue;
							}							
						} catch (JsonMappingException e) {
							System.out.println("2. JSON Mapping Exception Exception , hence error data inserting into error table ");
							e.printStackTrace();
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
							}
							continue;							
						} catch (IOException e) {							
							System.out.println("3. IO Exception Exception , hence error data  inserting into error table");
							if (!Strings.isNullOrEmpty(JsongetString.toString())) {
								String jsonErr = JsongetString.toString();
								String arr1[] = jsonErr.split(",");
								int re_Id= Integer.parseInt(arr1[1].replaceAll("[\"_:a-zA-Z]", ""));
								System.out.println("Appl_Req_Id => "+ re_Id);
								dbCon.insertErrData(re_Id,JsongetString, DupErrDataTable);
							}
							continue;
						}

						// Code to store the latest offset and its partition
						hm.put(intPartition_Number, intOffset);

						if (size == uniDataBatch.size()){
							prepareDupDataBatchQuery(comm, responseObj, JsongetString, intOffset, intPartition_Number,
									DupErrDataTable, dupErrTableColsList);
							System.out.println("1. DupErrDataTable : "+ DupErrDataTable);
						}						
						else{
							prepareBatchQuery(comm, responseObj, intOffset, intPartition_Number, ccwTable,
									mainTableColsList);
						}						
						size = uniDataBatch.size();

						if (i % 5 == 0) {
							long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
							consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));							
						}

						// Creating batch as per config prop file
						if (++i % batchSize == 0) {
							if (uniDataBatch.size() > 0 || dupDataBatchMain.size() > 0) {							
								
								if (uniDataBatch.size() > 0) {
									System.out.println("1. ========== Insert into Unique table ===========");
									dbCon.executeUniqueBatch(uniDataBatchMain, uniDataBatch, DupErrDataTable , dupErrTableColsList);
									getconn.updateCommitedOffset(hm, ccwCommitTable);
								}
								if (dupDataBatchMain.size() > 0) {
									System.out.println("1. =========Insert into Duplicate table =============");
									dbCon.executeBatchWithDuplicateData(dupDataBatchMain);
								}
								insertFlag = true;
							}

							// Reset Values
							insertFlag = true;
							uniDataBatch.clear();
							uniDataBatchMain.clear();
							dupDataBatchMain.clear();
							size = 0;
							
						}
						
					} // End For loop of Partiton records 			

					//Commit Offset after partiton records iteration
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}

				// Commit offset after for Poll Loop
				if (recCheck) {					
					getconn.updateNewOffset(hm, ccwCommitTable,topicName,groupId);
				}

				if (uniDataBatch.size() > 0 || dupDataBatchMain.size() > 0) {

					if (uniDataBatch.size() > 0) {
						System.out.println("2. ========== Insert into Unique table =======");
						dbCon.executeUniqueBatch(uniDataBatchMain, uniDataBatch , DupErrDataTable , dupErrTableColsList);
						getconn.updateCommitedOffset(hm, ccwCommitTable);
					}
					if (dupDataBatchMain.size() > 0) {
						System.out.println("2. ========= Insert into Duplicate table =========");
						dbCon.executeBatchWithDuplicateData(dupDataBatchMain);
					}
					insertFlag = true;
					
					// Clear fields					
					uniDataBatch.clear();
					uniDataBatchMain.clear();
					dupDataBatchMain.clear();
					size = 0;
					System.out.println("Insert flag : " + insertFlag + ", Unique batch Size : " + uniDataBatch.size()
							+ ", Duplicate batch Size : " + dupDataBatch.size());
					
				}		
				insertFlag = false;			
				
			} // end While
		} catch (Exception e) {
			System.out.println("Exception in Consumer class : " + e.getMessage());
			String errMsg = e.getMessage().toString();
			if(errMsg.contains("poll()")){
				System.out.println("True Contain");
				// Trigger mail
				AmpConsumer.main(argv);
			}else{
				System.out.println("Please check the other Ex exception");
			}
		} finally {
			consumer.close();
		}

	}
	
	public static void prepareBatchQuery(commonUtility commObj, AmpCounsumerRespBean obj, Integer offset,
			int Partition_Number, String Table, String colsList) {
		String query = null;
		try {
			query = "insert into " + Table + " (" + colsList + ") " + " values ( "
					+ commObj.toStringFormat(obj.getApplName()) + "," + obj.getAppReqId() + "," + commObj.toStringFormat(obj.getTrnxType())+  "," + commObj.toStringFormat(obj.getSubTnxType())+","
					+ commObj.toStringFormat(obj.getCCOUserId()) + ","
					+ obj.getInstanceId() + "," + obj.getContractNumber() + "," + obj.getSerLineId() + ","
					+ commObj.toStringFormat(obj.getSrcCpLineId()) 
					 + "," + commObj.dateFormat(obj.getTerDate()) + ","
					+ offset + "," + Partition_Number + ")";
			if (!Strings.isNullOrEmpty(query)) {
				 //System.out.println("Query : "+ query);
				uniDataBatchMain.add(query);
			}
		} catch (Exception e) {
			System.out.println("Exception in prepareBatchQuery : " + e.getMessage());
		}
	}

	public static void prepareDupDataBatchQuery(commonUtility commObj, AmpCounsumerRespBean obj, String jsonData,
			Integer offset, int Partition_Number, String Table, String colsList) {
		String query = null;
		try {
			query = "insert into " + Table + " (" + colsList + ")" + " values (" + obj.getAppReqId() + ",'" + jsonData
					+ "', sysdate )";
			if (!Strings.isNullOrEmpty(query)) {
				//System.out.println("Duplicate Query  : " + query);
				dupDataBatchMain.add(query);
			}
		} catch (Exception e) {
			System.out.println("Exception in prepareBatchQuery : " + e.getMessage());
		}
	}

}
