package CumsumerClasses;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import java.awt.dnd.DnDConstants;
//import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.JsonMappingException;
//import net.minidev.json.parser.JSONParser;
//import net.minidev.json.parser.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utility.commonUtility;
import com.utility.dbConnection;
import CumsumerClasses.ConsumerResponseBean;
import joptsimple.internal.Strings;

public class ConsumerOffset {
	
	static KafkaConsumer<String, String> consumer = null;
	static Map<Integer, Integer> hm = null;
	static dbConnection dbCon = null;
	static String ccwTable = null;
	static String ccwCommitTable = null;
	static List<String> batch = new ArrayList<String>();
	static int batchSize = 0;
	public static void main(String[] argv) throws Exception {


		
		commonUtility comm = new commonUtility();
		Properties prop = comm.readProp();
		
		// fetching properties data as input
		String topicName = prop.getProperty("topicName");
		String groupId = prop.getProperty("groupId");
		ccwTable = prop.getProperty("ccwTable");;
		ccwCommitTable = prop.getProperty("ccwCommitTable");
		String strServerName = prop.getProperty("bootstrapServer");
		batchSize = Integer.parseInt(prop.getProperty("BatchSize").toString());
		// Printing Confing file Data
		System.out.println("================================================");
		System.out.println("topicName : "+ topicName);
		System.out.println("groupId : "+ groupId);
		System.out.println("Server : "+ strServerName);
		System.out.println("ccwTable : "+ ccwTable);
		System.out.println("ccwCommitTable : "+ ccwCommitTable);
		System.out.println("Batch Size : "+ batchSize);
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

		// print the topic name
		System.out.println("Subscribed to topic " + topicName);

		ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
				false);
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
						for(@SuppressWarnings("rawtypes") Map.Entry e: hm.entrySet()){
							if(Integer.parseInt(e.getKey().toString())== topicPartition.partition()){
								cFlag = false;
								break;
							}
						}
							
						if(cFlag){
							System.out.println("New Partion Number is found : "+ topicPartition.partition());
								try {
									hm.put(topicPartition.partition(),  (int) consumer.position(topicPartition));
									dbCon.insertCommitedOffset(topicPartition.partition(), consumer.position(topicPartition), ccwCommitTable);
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
							} 
						else {						
								
							// Seeking data as per partition and offset 	
							long currOffset = consumer.position(topicPartition);
							long commitOffset = Integer
									.parseInt((consumer.committed(topicPartition).toString()).replaceAll("[^0-9]", ""));
							
							System.out.println(topicPartition.partition() + " =>  Partition : " + topicPartition.partition() + ", Offset : "
									+ hm.get(topicPartition.partition()));
	
							if (hm.get(topicPartition.partition()) == 0) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from Current offset");
								consumer.seek(topicPartition, currOffset);
							} else if (hm.get(topicPartition.partition()) != 0) {
								System.out.println(topicPartition.partition()
										+ " Partition, Reading from table commit offset");
								consumer.seek(topicPartition, hm.get(topicPartition.partition()) + 1);
							} else if ((!Strings
									.isNullOrEmpty(strCheck = consumer.committed(topicPartition).toString()))) {
								System.out.println(
										topicPartition.partition() + " Partition, Reading from Commit offset");
								consumer.seek(topicPartition, commitOffset + 1);
							} else								
								System.out.println("No Offset Matched.");							
	
						}
					}
				}
			});

			// Loop to Poll data from partion
			boolean insertFlag = false;
			while (true) {
				
				dbConnection getconn = new dbConnection();
				ConsumerRecords<String, String> records = consumer.poll(1);
				
				// Partion processing
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
									
					
					//System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Next Batch from Partiton @@@@@@@@@@@@@@@@@@@@@@@@@@@@ ");
					// Processing records from individual partition
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.println(record.partition() + " || " + record.offset() + " || " + record.value());
						String JsongetString = new String(record.value());
						int intPartition_Number = record.partition();
						Integer intOffset = (int) record.offset();

						ConsumerResponseBean responseObj = null;
						try {

							objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
							objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
							responseObj = objectMapper.readValue(JsongetString, ConsumerResponseBean.class);
						} catch (JsonParseException e) {
							e.printStackTrace();
						} catch (JsonMappingException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}

						// Code to store the latest offset and its partition
						// number in CommitOffset table
						hm.put(intPartition_Number, intOffset);						
			
						System.out.println("============== calling prepareBatchQuery =================");
						//getconn.insertIntoDatabase(responseObj, intOffset, intPartition_Number, ccwTable);
						prepareBatchQuery(comm, responseObj, intOffset, intPartition_Number, ccwTable);
						System.out.println("Record Number => " + i++);

						if (i % 5 == 0) {
							long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
							consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
							getconn.updateCommitedOffset(hm, ccwCommitTable);
						}
						
						// Creating batch of 100 records
						if(i % batchSize == 0){
							//System.out.println("*************** Calling Batch execute query *****************");
							dbCon.executeBatch(batch);
							insertFlag = true;
							batch.clear();
							//System.out.println("*************************************************************");
						}
					}  // End Partiton records for
					
					System.out.println("Insert flag : "+ insertFlag + ",  batch Size : "+ batch.size());
					if(batch.size() > 0){
						System.out.println("Calling batch execute query due to records <> 20 and loop terminated. Batch Size : "+ batch.size());
						System.out.println("*************** Calling Batch execute query **************************");
						dbCon.executeBatch(batch);
						insertFlag = true;
						System.out.println("*************************************************************");
					}
						
					// Clear fields
					//System.out.println("============== Clear fields =============================");
					insertFlag = false;
					batch.clear();
					System.out.println("insertflag : "+ insertFlag + ", Batch Size : "+ batch.size());
					//sSystem.out.println("==================== End Clear fields ==================");
					
					// Commit Offset after partiton records iteration
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}

				// Commit offset after for Poll Loop
				getconn.updateCommitedOffset(hm, ccwCommitTable);
				getconn.updateNewOffset(hm, ccwCommitTable);

				// Control on poll() Count
				if (i > 2) {
					System.out.println("Breaking while loop...!!");
					break;
				}

			} // end While
		} catch (Exception e) {
			System.out.println("Exception in Consumer class : " + e.getMessage());
		} finally {
			consumer.close();
		}
	
		
		
	}

	public static void prepareBatchQuery(commonUtility commObj, ConsumerResponseBean obj, Integer offset, int Partition_Number, String ccwTable2){
		String query = null;
		try{
		 query = "insert into "+ccwTable+"(WEB_ORDER_ID,VIRTUAL_ACCOUNT_ID,UPDATED_ON,UPDATED_BY,SMART_ACCOUNT_ID, ORDER_SOURCE,"
				+ "ORDER_LINE_ID,ORDER_ID,HOLDING_VIRTUAL_ACCOUNT_ID,HOLDING_SMART_ACCOUNT_ID,GG_ENQUEUE_TIME,GG_DEQUEUE_TIME,"
				+ " ERP_SO_NUMBER,ERP_SO_LINE_NUMBER,CREATED_ON,CREATED_BY,ASSIGNMENT_EVENT,ACTIVE,ACCOUNT_TYPE, REC_INSERT_DATE, OFFSET ,PARTITION_NUMBER )"
				+ " values ( " + obj.getWebOrdId() + "," + obj.getVrtlActId() + ","
				+ commObj.dateFormat(obj.getUpdOn()) + "," + commObj.toStringFormat(obj.getUpdBy()) + ","
				+ obj.getSmartActId() + "," + commObj.toStringFormat(obj.getOrdSrc()) + "," + obj.getLnId()
				+ "," + obj.getOrdId() + "," + obj.getHldVrtlSmrtActId() + "," + obj.getSmartActId() + ","
				+ commObj.dateFormat(obj.getGgEqTm()) + "," + commObj.dateFormat(obj.getGgDqTm()) + ","
				+ obj.getErpSoNum() + "," + obj.getErpSoLnNum() + "," + commObj.dateFormat(obj.getCrOn()) + ","
				+ commObj.toStringFormat(obj.getCrBy()) + "," + commObj.toStringFormat(obj.getAsgmtEvnt()) + ","
				+ obj.getActive() + "," + commObj.toStringFormat(obj.getAcctTyp()) + ",sysdate," + offset + ","
				+ Partition_Number + ")";
		
		if(! Strings.isNullOrEmpty(query)){
			System.out.println("Query  : "+ query);
			batch.add(query);
		}
		
		}catch(Exception e){
			System.out.println("Exception in prepareBatchQuery : "+ e.getMessage());
		}
	}
	
	
}