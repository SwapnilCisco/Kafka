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
import CumsumerClasses.AmpCounsumerRespBean;

public class AmpConsumer {
		
		static KafkaConsumer<String, String> consumer = null;
		static Map<Integer, Integer> hm = null;
		static dbConnection dbCon = null;
		static String ccwTable = null;
		static String ccwCommitTable = null;
		static List<String> batch = new ArrayList<String>();
		static int batchSize = 0;
		static int Poll_ms = 0;
		static String ErrDataTable = null;
		static String BackupTable = null;
		static String DupErrDataTable = null;
		public static void main(String[] argv) throws Exception {
			
			commonUtility comm = new commonUtility();
			Properties prop = comm.readProp();
			
			// fetching properties data as input
			String topicName = prop.getProperty("AmpTopicname");
			String groupId = prop.getProperty("AmpGroupId");
			ccwTable = prop.getProperty("AmpApptable");
			ccwCommitTable = prop.getProperty("AmpCommitTable");
			String strServerName = prop.getProperty("AmpBootstrapServer");
			batchSize = Integer.parseInt(prop.getProperty("BatchSize").toString());
			Poll_ms = Integer.parseInt(prop.getProperty("Poll_Ms").toString());
			ErrDataTable = prop.getProperty("AMPErrDataTable");
			BackupTable = prop.getProperty("AmpApptableBackup");
			DupErrDataTable = prop.getProperty("AMPDupErrDataTable");
			// Printing Confing file Data
			System.out.println("================================================");
			System.out.println("topic Name : "+ topicName);
			System.out.println("group Id : "+ groupId);
			System.out.println("Server : "+ strServerName);
			System.out.println("Table : "+ ccwTable);
			System.out.println("Commit Table : "+ ccwCommitTable);
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
									//consumer.seekToBeginning(partitions);
									consumer.seek(topicPartition, currOffset);  // uncomment after testing
								} else if (hm.get(topicPartition.partition()) != 0) {
									System.out.println(topicPartition.partition()
											+ " Partition, Reading from table commit offset");
									consumer.seekToBeginning(partitions);
									//consumer.seek(topicPartition, hm.get(topicPartition.partition()) + 1);
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
				boolean recCheck = false;
				int pollCount = 0;
				while (true) {
					System.out.println("Poll Count : "+ pollCount++);
					dbConnection getconn = new dbConnection();
					ConsumerRecords<String, String> records = consumer.poll(Poll_ms);//Poll_ms);
					recCheck = false;
					// Partion processing
					for (TopicPartition partition : records.partitions()) {
						List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);									
						
						
						// Processing records from individual partition
						for (ConsumerRecord<String, String> record : partitionRecords) {
							recCheck = true;
							System.out.println(record.partition() + " || " + record.offset() + " || " + record.value());
							String JsongetString = new String(record.value());
							System.out.println("JsongetString : "+ JsongetString);
							
							//JsongetString.
							//System.out.println("JsongetString : "+ JsongetStri);
							int intPartition_Number = record.partition();
							Integer intOffset = (int) record.offset();

							AmpCounsumerRespBean responseObj = null;
							try {

								objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
								//objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
								responseObj = objectMapper.readValue(JsongetString, AmpCounsumerRespBean.class);								
																
							} catch (JsonParseException e) {
								System.out.println("Error data hence inserting into table");
								if (!Strings.isNullOrEmpty(JsongetString)) {
									dbCon.insertErrData(JsongetString, ErrDataTable);
									continue;
								}
								//e.printStackTrace();
							} catch (JsonMappingException e) {
								System.out.println("Error data hence inserting into table");
								dbCon.insertErrData(JsongetString, ErrDataTable);
								continue;
								//e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}

							// Code to store the latest offset and its partition
							// number in CommitOffset table
							hm.put(intPartition_Number, intOffset);						
				
							System.out.println("============== calling prepareBatchQuery =================");							
							prepareBatchQuery(comm, responseObj, intOffset, intPartition_Number, BackupTable);
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
								dbCon.copyTableData(BackupTable, ccwTable, DupErrDataTable);
								dbCon.deleteTableData(BackupTable);
								insertFlag = true;
								batch.clear();
							
							}
						}  // End Partiton records for
						
						System.out.println("Insert flag : "+ insertFlag + ",  batch Size : "+ batch.size());
						if(batch.size() > 0){
							System.out.println("Calling batch execute query due to records <> 20 and loop terminated. Batch Size : "+ batch.size());
							System.out.println("*************** Calling Batch execute query **************************");
							dbCon.executeBatch(batch);
							dbCon.copyTableData(BackupTable, ccwTable, DupErrDataTable);
							dbCon.deleteTableData(BackupTable);
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
					if(recCheck){
						getconn.updateCommitedOffset(hm, ccwCommitTable);
						getconn.updateNewOffset(hm, ccwCommitTable);
					}
					// Control on poll() Count
					if (i > 2) {
						System.out.println("Breaking while loop...!!");
						break;
					}
					
					if(pollCount > 100)
					{
						System.out.println("No data returing from Poll hence terminating program.");
						break;
					}
				} // end While
			} catch (Exception e) {
				System.out.println("Exception in Consumer class : " + e.getMessage());
			} finally {
				consumer.close();
			}
		
			
			
		}

		public static void prepareBatchQuery(commonUtility commObj, AmpCounsumerRespBean obj, Integer offset, int Partition_Number, String ccwTable){
			String query = null;
			try{
			 query = "insert into "+ccwTable+" (APPL_NAME, APPL_REQUEST_ID, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID, "
					 + " INSTANCE_ID, CONTRACT_NUMBER, SERVICE_LINE_ID, SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number) "
					+ " values ( "
					+ commObj.toStringFormat(obj.getApplName()) + "," + obj.getAppReqId() + ","
					+  commObj.toStringFormat(obj.getCCOUserId()) + "," +obj.getContractNumber() + "," + obj.getInstanceId()
					+ "," + obj.getSerLineId()  + "," + commObj.toStringFormat(obj.getSrcCpLineId()) + "," + commObj.toStringFormat(obj.getSubTnxType())  + ","
					+ commObj.toStringFormat(obj.getTrnxType()) + "," + commObj.dateFormat(obj.getTerDate())+","+offset+","+Partition_Number
					+ ")";
			
			if(! Strings.isNullOrEmpty(query)){
				System.out.println("Query  : "+ query);
				batch.add(query);
			}
			
			}catch(Exception e){
				System.out.println("Exception in prepareBatchQuery : "+ e.getMessage());
			}
		}
		
		


}
