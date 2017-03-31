package CumsumerClasses;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.utility.dbConnection;
import com.utility.commonUtility;
import joptsimple.internal.Strings;

public class Consumer_ccw_orders_sava {

	static KafkaConsumer<String, String> consumer = null;
	static Map<Integer, Integer> hm = null;
	static dbConnection dbCon = null;
	static String ccwTable = null;
	static String ccwCommitTable = null;
	static int recInsertCount = 0;

	public static void main(String[] args) throws Exception {
		
		commonUtility comm = new commonUtility();
		Properties prop = comm.readProp();
		
		// fetching properties data as input
		String topicName = prop.getProperty("topicName");
		String groupId = prop.getProperty("groupId");
		ccwTable = prop.getProperty("ccwTable");;
		ccwCommitTable = prop.getProperty("ccwCommitTable");
		String strServerName = prop.getProperty("bootstrapServer");
		
		// Printing Confing file Data
		System.out.println("================================================");
		System.out.println("topicName : "+ topicName);
		System.out.println("groupId : "+ groupId);
		System.out.println("Server : "+ strServerName);
		System.out.println("ccwTable : "+ ccwTable);
		System.out.println("ccwCommitTable : "+ ccwCommitTable);
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
			while (true) {
				
				dbConnection getconn = new dbConnection();
				ConsumerRecords<String, String> records = consumer.poll(1);

				// Partion processing
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

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
			
						System.out.println("calling insertIntoDatabase");
						getconn.insertIntoDatabase(responseObj, intOffset, intPartition_Number, ccwTable);
						System.out.println("Insert Record Number => " + i++);

						if (i % 5 == 0) {
							long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
							consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
							//getconn.updateCommitedOffset(hm, ccwCommitTable);
						}
					}

					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}

				// Commit offset after for Poll Loop
				getconn.updateCommitedOffset(hm, ccwCommitTable);
				getconn.updateNewOffset(hm, ccwCommitTable);

				// Control on poll() Count
				if (i > 1) {
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
	
	public void prepareBatchQuery(){
		
	}

}
