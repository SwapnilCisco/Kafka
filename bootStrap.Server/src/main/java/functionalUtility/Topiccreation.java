package functionalUtility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import com.utility.commonUtility;
import com.utility.dbConnection;

import joptsimple.internal.Strings;

//Create java class named “SimpleProducer”
public class Topiccreation {

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();
		// String topicName = "XXCSS_O.XXCSS_AMP_MACD_REQUEST";
		String topicName = "XXCSS_AMP_TEST_CONSUMER1";

		// create instance for properties to access producer configs
		Properties props = new Properties();
		// Assign localhost id
		props.put("bootstrap.servers", "amp-stg-01:9092");
		// Set acknowledgements for producer requests.
		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);
		// Specify buffer size in config
		props.put("batch.size", 10);
		// Reduce the no of requests less than 0
		props.put("linger.ms", 100);

		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("buffer.memory", 93554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props); // 200001
																						// 58425773629
																						// 1829658510
		// 58425773337 1829658321

		String con_num;
		String ins_id;
		String cpl_id;
		String ser_id;
		int seqid;

		try {

			// dbConnection getconn = new dbConnection();
			// List<AMPConsumerBean> data = getconn.getIntoDatabase();
			// -------------------
			// commonUtility commObj = new commonUtility();
			// System.out.println("Inside insertIntoDatabase function..!!");
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// System.out.println("1");
			/*
			 * Connection con = DriverManager.getConnection(
			 * "jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(TRANSPORT_CONNECT_TIMEOUT"+
			 * "=3)(RETRY_COUNT=1)(ADDRESS_LIST=(LOAD_BALANCE=ON)(FAILOVER=ON)(ADDRESS=(PROTOCOL"+
			 * "=TCP)(HOST=scan-nprd-2023)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=DV1CSF_SRVC_O"+
			 * "TH.cisco.com)(SERVER=DEDICATED)))","APPS","P0t3ntial");
			 */

			Connection con = DriverManager.getConnection(
					"jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(TRANSPORT_CONNECT_TIMEOUT=3)(RETRY_COUNT=1)(ADDRESS_LIST=(LOAD_BALANCE=ON)(FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.69.52)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.69.53)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=173.38.69.54)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=TS3CSF_SRVC_OTH.cisco.com)(SERVER=DEDICATED)))",
					"XXCTS_DM", "Hw4N4b5B");

			// P0t3ntial refrig3r8
			// System.out.println("2");
			Statement stmt = con.createStatement();

			String query = null;
			try {
				/// Table Name : xxcss_scm_ccw_orders, xxcss_scm_ccw_orders_temp
				/*
				 * query =
				 * "insert into xxcss_scm_ccw_orders_temp(WEB_ORDER_ID,VIRTUAL_ACCOUNT_ID,UPDATED_ON,UPDATED_BY,SMART_ACCOUNT_ID, ORDER_SOURCE,"
				 * +
				 * "ORDER_LINE_ID,ORDER_ID,HOLDING_VIRTUAL_ACCOUNT_ID,HOLDING_SMART_ACCOUNT_ID,GG_ENQUEUE_TIME,GG_DEQUEUE_TIME,"
				 * +
				 * " ERP_SO_NUMBER,ERP_SO_LINE_NUMBER,CREATED_ON,CREATED_BY,ASSIGNMENT_EVENT,ACTIVE,ACCOUNT_TYPE, REC_INSERT_DATE, OFFSET ,PARTITION_NUMBER )"
				 * + " values ( "+
				 * obj.getWebOrdId()+","+obj.getVrtlActId()+","+commObj.
				 * dateFormat(obj.getUpdOn())
				 * +","+commObj.toStringFormat(obj.getUpdBy())+","+obj.
				 * getSmartActId()+","+commObj.toStringFormat(obj.getOrdSrc())+
				 * ","+obj.getLnId()+","+obj.getOrdId()+","+obj.
				 * getHldVrtlSmrtActId()+","+obj.getSmartActId()+","+commObj.
				 * dateFormat(obj.getGgEqTm())+","+commObj.dateFormat(obj.
				 * getGgDqTm())+
				 * ","+obj.getErpSoNum()+","+obj.getErpSoLnNum()+","+commObj.
				 * dateFormat(obj.getCrOn())+","+commObj.toStringFormat(obj.
				 * getCrBy())+","+commObj.toStringFormat(obj.getAsgmtEvnt())+","
				 * +obj.getActive()+","+commObj.toStringFormat(obj.getAcctTyp())
				 * + ",sysdate,"+offset+","+Partition_Number+")";
				 */

				// query = "select /*+parallel(a,20)*/
				// a.contract_number,b.covered_line_id,b.instance_id,b.service_line_id
				// , xxcss_amp_input_temp.nextval from apps.xxccs_ds_sahdr_core
				// a, apps.xxccs_ds_cvdprdline_detail b "
				// + " where a.contract_number ='93796136'"
				// + " and b.instance_id ='1505957551'"
				// + " and b.contract_id = a.contract_id"
				// + " and b.service_line_id = a.service_line_id"
				// + " and not exists (select 1 from apps.okc_k_lines_b"
				// + " where id = b.covered_line_id"
				// + " and sts_code not in ('TERMINATED','EXPIRED')"
				// + " and DATE_TERMINATED is not null) and rownum <= 1";

				query = "select /*+parallel(a,20)*/ a.contract_number,b.covered_line_id,b.instance_id,b.service_line_id , xxcss_amp_input_temp.nextval, a.SERVICE_LINE_NAME from apps.xxccs_ds_sahdr_core a, apps.xxccs_ds_cvdprdline_detail b "
						+ " where a.contract_number ='1005862' "
						+ " and b.instance_id ='1796233456' " + " and b.contract_id = a.contract_id"
						+ " and b.service_line_id = a.service_line_id"
						+ " and not exists (select 1 from apps.okc_k_lines_b" + " where id = b.covered_line_id"
						+ " and sts_code not in ('TERMINATED','EXPIRED')"
						+ " and DATE_TERMINATED is not null) and rownum <= 1";

			} catch (NullPointerException e) {
				System.out.println("Null Ponter Exception thrown");
			}

			// ArrayList<AMPConsumerBean> listStrings = new
			// ArrayList<AMPConsumerBean>();
			int j = 0;
			System.out.println("Query : " + query);
			if (!Strings.isNullOrEmpty(query)) {
				ResultSet rsmt = stmt.executeQuery(query);
				while (rsmt.next()) {
					// AMPConsumerBean ampbean = new AMPConsumerBean();
					// ampbean.setContract_number(rsmt.getString(1));
					// ampbean.setInstance_id(rsmt.getString(3));
					// ampbean.setSource_cp_line_id(rsmt.getString(2));
					// ampbean.setService_line_id(rsmt.getString(4));
					// ampbean.setSeq_id(rsmt.getInt(5));
					// listStrings.add(ampbean);
					/*
					 * System.out.println("J : "+ j++); System.out.println(
					 * " rsmt.getString(4) : " + rsmt.getString(4));
					 * System.out.println(" rsmt.getString(3) : " +
					 * rsmt.getString(3)); System.out.println(
					 * " rsmt.getString(2) : " + rsmt.getString(2));
					 * System.out.println(" rsmt.getString(1) : " +
					 * rsmt.getString(1));
					 */

					System.out.println(" Instance ID  : " + rsmt.getString(3));

					// String str =
					// "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+rsmt.getInt(5)+"\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+rsmt.getString(1)+"\",\"INSTANCE_ID\":\""+rsmt.getString(3)+"\",\"SERVICE_LINE_ID\":\""+rsmt.getString(4)+"\",\"SOURCE_CP_LINE_ID\":\""+rsmt.getString(2)+"\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"TERMINATION_DATE\":\"2017-04-03\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

					//String str =
					//"{\"APPL_NAME\":\"XXCSS_AMP\",\"APPL_REQUEST_ID\":\"55553333\",\"CCO_USER_ID\":\"ABABAR\",\"CONTRACT_NUMBER\":\"1005862\",\"INSTANCE_ID\":\"1796233456\",\"SERVICE_LINE_ID\":\"58089605575\",\"SOURCE_CP_LINE_ID\":\"58089605804\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"REASON_CODE\":\"OTHER\",\"TERMINATION_DATE\":\"2017-04-14\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

					// String str =
					// "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+rsmt.getInt(5)+"\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+rsmt.getString(1)+"\",\"INSTANCE_ID\":\""+rsmt.getString(3)+"\",\"SERVICE_LINE_ID\":\""+rsmt.getString(4)+"\",\"SOURCE_CP_LINE_ID\":\""+rsmt.getString(2)+"\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"REASON_CODE\":\"OTHER\",\"TERMINATION_DATE\":\"2017-04-11\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

					 String str =
					"{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+rsmt.getInt(5)+"\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+rsmt.getString(1)+"\",\"INSTANCE_ID\":\""+rsmt.getString(3)+"\",\"SERVICE_LINE_ID\":\""+
					rsmt.getString(4)+"\",\"SERVICE_LEVEL\":\""+ rsmt.getString(6)+"\",\"SOURCE_CP_LINE_ID\":\""+rsmt.getString(2)+"\",\"SUB_TRX_TYPE\":\"C2C_AT_PRODUCT\",\"TERMINATION_DATE\":\"2017-04-03\",\"TARGET_CONTRACT_NUMBER\":\"\",\"REASON_CODE\":\"C2C-SAME/SIMILARSVC LVL\",\"TRANSACTION_TYPE\":\"MOVE\"}";
					
					
					 producer.send(new ProducerRecord<String,
					 String>(topicName,Integer.toString(j), str));

				}
				con.commit();
				con.close();
				rsmt.close();
				
				// hard coded Run // Commented by vivek
				if(true){
					//String str = "{\"APPL_NAME\":\"XXCSS_AMP\",\"APPL_REQUEST_ID\":\"55553335\",\"CCO_USER_ID\":\"ABABAR\",\"CONTRACT_NUMBER\":\"1005862\",\"INSTANCE_ID\":\"895442977\",\"SERVICE_LINE_ID\":\"53190017389\",\"SOURCE_CP_LINE_ID\":\"53190017649\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"REASON_CODE\":\"OTHER\",\"TERMINATION_DATE\":\"2017-04-14\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

					// String str =
					//		 "{\"APPL_NAME\":\"XXCSS_AMP\",\"APPL_REQUEST_ID\":\"55553335\",\"CCO_USER_ID\":\"ABABAR\",\"CONTRACT_NUMBER\":\"1005862\",\"INSTANCE_ID\":\"1796233456\",\"SERVICE_LINE_ID\":\"58089605575\",\"SERVICE_LEVEL\":\"SNT\",\"SOURCE_CP_LINE_ID\":\"58089605804\",\"SUB_TRX_TYPE\":\"C2C_AT_PRODUCT\",\"TERMINATION_DATE\":\"2017-05-14\",\"TARGET_CONTRACT_NUMBER\":\"\"REASON_CODE\":\"C2C-SAME/SIMILAR SVC LVL\",\"TRANSACTION_TYPE\":\"MOVE\"}";

					// String str =
					// "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+rsmt.getInt(5)+"\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+rsmt.getString(1)+"\",\"INSTANCE_ID\":\""+rsmt.getString(3)+"\",\"SERVICE_LINE_ID\":\""+rsmt.getString(4)+"\",\"SOURCE_CP_LINE_ID\":\""+rsmt.getString(2)+"\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"REASON_CODE\":\"OTHER\",\"TERMINATION_DATE\":\"2017-04-11\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

					// String str =
					// "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+rsmt.getInt(5)+"\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+rsmt.getString(1)+"\",\"INSTANCE_ID\":\""+rsmt.getString(3)+"\",\"SERVICE_LINE_ID\":\""+
					// rsmt.getString(4)+"\",\"SERVICE_LEVEL\":\""+
					// rsmt.getString(6)+"\",\"SOURCE_CP_LINE_ID\":\""+rsmt.getString(2)+"\",\"SUB_TRX_TYPE\":\"C2C_AT_PRODUCT\",\"TERMINATION_DATE\":\"2017-04-03\",\"TARGET_CONTRACT_NUMBER\":\"95609736\",\"REASON_CODE\":\"C2C-SAME/SIMILAR
					// SVC LVL\",\"TRANSACTION_TYPE\":\"MOVE\"}";
					 

					 String str = 
						"{\"APPL_NAME\":\"XXCSS_AMP\",\"APPL_REQUEST_ID\":\"55553335\",\"CCO_USER_ID\":\"ABABAR\",\"CONTRACT_NUMBER\":\"1005862\",\"INSTANCE_ID\":\"1796233456\",\"SERVICE_LEVEL\":\"SNT\",\"SOURCE_CP_LINE_ID\":\"58089605804\",\"SUB_TRX_TYPE\":\"C2C_AT_PRODUCT\",\"TARGET_CONTRACT_NUMBER\":\"58089605804\",\"REASON_CODE\":\"C2C-SAME/SIMILARSVC LVL\",\"TRANSACTION_TYPE\":\"MOVE\"}";


					 producer.send(new ProducerRecord<String,
					 String>(topicName,Integer.toString(j), str));
				}
			}
			// ------------------
			/*
			 * System.out.println("data.size() --> "+data.size()); for (int i=0;
			 * i<data.size(); i++){ AMPConsumerBean row = data.get(i); con_num =
			 * row.getContract_number(); cpl_id = row.getSource_cp_line_id();
			 * ins_id = row.getInstance_id(); ser_id = row.getService_line_id();
			 * seqid = row.getSeq_id(); //System.out.println("Element "
			 * +i+Arrays.toString(row));
			 * 
			 * String str =
			 * "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\""+seqid+
			 * "\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\""+con_num+
			 * "\",\"INSTANCE_ID\":\""+ins_id+"\",\"SERVICE_LINE_ID\":\""+ser_id
			 * +"\",\"SOURCE_CP_LINE_ID\":\""+cpl_id+
			 * "\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"TERMINATION_DATE\":\"2017-03-29\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";
			 * // System.out.println("str  ::  "+ str); producer.send(new
			 * ProducerRecord<String, String>(topicName, Integer.toString(i),
			 * str));
			 * 
			 * System.out.println("i --> "+i);
			 * 
			 * }
			 */
		} finally {
			System.out.println("Breaking while loop...!!");
			// consumer.close();
		}

		// String str1 =
		// "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"200010\",\"CCO_USER_ID\":\"SYEBASHA\",\"CONTRACT_NUMBER\":\"1004107\",\"INSTANCE_ID\":\"1830379312\",\"SERVICE_LINE_ID\":\"57047959884\",\"SOURCE_CP_LINE_ID\":\"58433970934\",\"SUB_TRX_TYPE\":\"COVERAGE\",\"TERMINATION_DATE\":\"2017-03-29\",\"TRANSACTION_TYPE\":\"TERMINATE\"}";

		// System.out.println("str :: "+ str1);

		/*
		 * for(int i = 1; i < 2; i++) producer.send(new ProducerRecord<String,
		 * String>(topicName, Integer.toString(i), str));
		 */
		// long startTime = System.currentTimeMillis();
		System.out.println("Message sent successfully");
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("totalTime :: " + totalTime);
		double time_seconds = totalTime / 1000.0; // add the decimal/ 60;
		System.out.println("time_seconds :: " + time_seconds);
		double time_minutes = (totalTime / 1000.0) / 60;
		System.out.println("time_minutes :: " + time_minutes);
		producer.close();
	}
}
