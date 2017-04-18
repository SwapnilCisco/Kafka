package com.utility;

import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import joptsimple.internal.Strings;

public class dbConnection {

	static Connection con = null;

	public Statement dbConn() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			 con = DriverManager
					.getConnection("jdbc:oracle:thin:@scan-nprd-2023:1541/DV1CSF_SRVC_OTH.cisco.com", "APPS", "P0t3ntial");
			Statement stmt = con.createStatement();
			return stmt;
		} catch (Exception e) {
			System.err.println("Exception in dbConn"+ e.getMessage() );
			e.getStackTrace();
			//System.exit(0);
			return null;
		}
	}
	
	public Statement DBConnect(String UN, String Pass, String URL) {
		try {
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager
					.getConnection(URL, UN, Pass);
			Statement stmt = con.createStatement();
			return stmt;
		} catch (Exception e) {
			System.out.println("Exception in dbConn");
			return null;
		}

	}

	public HashMap<Integer, Integer> executeQuery(String query) {
		HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
		try {
			System.out.println("inside executeQuery function");
			Statement stmt = dbConn();
			String query1 = null;
			try {
				query1 = query;

			} catch (NullPointerException e) {
				System.out.println("Null Pointer Exception thrown");
			}

			System.out.println("Query : " + query1);
			if (!Strings.isNullOrEmpty(query1)) {
				ResultSet rs1 = stmt.executeQuery(query1);
				// System.out.println("Query result : "+ rs1.last());
				while (rs1.next()) {
					System.out.println(
							"Commited Offset found in Table => Partition : " + rs1.getInt("PARTITION_NUMBER") + " , Offset : " + rs1.getInt("OFFSET"));
					hm.put(rs1.getInt("PARTITION_NUMBER"), rs1.getInt("OFFSET"));
				}
				con.close();
				return hm;
			}
		} catch (Exception e) {
			System.out.println("Exception in executeQuery : " + e.getStackTrace());
			e.printStackTrace();
			// con.close();
		}
		return null;
	}
/*
	public void insertIntoDatabase(ConsumerResponseBean obj, long offset, int Partition_Number, String ccwTable) {
		try {
			commonUtility commObj = new commonUtility();
			// System.out.println("Inside insertIntoDatabase function..!!");
			//Class.forName("oracle.jdbc.driver.OracleDriver");
			// System.out.println("1");
//			Connection con = DriverManager
//					.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(TRANSPORT_CONNECT_TIMEOUT"
//							+ "=3)(RETRY_COUNT=1)(ADDRESS_LIST=(LOAD_BALANCE=ON)(FAILOVER=ON)(ADDRESS=(PROTOCOL"
//							+ "=TCP)(HOST=scan-nprd-2023)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=DV1CSF_SRVC_O"
//							+ "TH.cisco.com)(SERVER=DEDICATED)))", "APPS", "P0t3ntial");
//			
			// System.out.println("2");
			
			Statement stmt = dbConn(); //con.createStatement();

			String query = null;
			try {
				/// Table Name : xxcss_scm_ccw_orders, xxcss_scm_ccw_orders_temp
				query = "insert into "+ccwTable+"(WEB_ORDER_ID,VIRTUAL_ACCOUNT_ID,UPDATED_ON,UPDATED_BY,SMART_ACCOUNT_ID, ORDER_SOURCE,"
						+ "ORDER_LINE_ID,ORDER_ID,HOLDING_VIRTUAL_ACCOUNT_ID,HOLDING_SMART_ACCOUNT_ID,GG_ENQUEUE_TIME,GG_DEQUEUE_TIME,"
						+ " ERP_SO_NUMBER,ERP_SO_LINE_NUMBER,CREATED_ON,CREATED_BY,ASSIGNMENT_EVENT,ACTIVE,ACCOUNT_TYPE, REC_INSERT_DATE, OFFSET ,PARTITION_NUMBER )"
						+ " values ( " + obj.getWebOrdId() + "," + obj.getVrtlActId() + ","
//						+ commObj.dateFormat(obj.getUpdOn()) + "," + commObj.toStringFormat(obj.getUpdBy()) + ","
						+ obj.getSmartActId() + "," + commObj.toStringFormat(obj.getOrdSrc()) + "," + obj.getLnId()
						+ "," + obj.getOrdId() + "," + obj.getHldVrtlSmrtActId() + "," + obj.getSmartActId() + ","
//						+ commObj.dateFormat(obj.getGgEqTm()) + "," + commObj.dateFormat(obj.getGgDqTm()) + ","
//						+ obj.getErpSoNum() + "," + obj.getErpSoLnNum() + "," + commObj.dateFormat(obj.getCrOn()) + ","
						+ commObj.toStringFormat(obj.getCrBy()) + "," + commObj.toStringFormat(obj.getAsgmtEvnt()) + ","
						+ obj.getActive() + "," + commObj.toStringFormat(obj.getAcctTyp()) + ",sysdate," + offset + ","
						+ Partition_Number + ")";
			} catch (NullPointerException e) {
				System.out.println("Null Pointer Exception thrown");
			}

			// System.out.println("Query : "+ query);
			if (!Strings.isNullOrEmpty(query)) {
				stmt.execute(query);
				con.commit();
				con.close();
			}

		} catch (Exception e) {
			System.out.println("Something went wrong inexecuting query");
			e.printStackTrace();
		}
	}
	
	*/
	
	
	public void insertErrData(int appl_req_id , String Json ,String errTable) {
		try {			
			Statement stmt = dbConn(); 
			String query = "insert into "+errTable+" values ("+appl_req_id+",'"+Json+"',sysdate)";
			System.out.println("Err Qruey : "+ query);
			if (!Strings.isNullOrEmpty(query)) {
				stmt.execute(query);
				con.commit();
				con.close();
			}
			
		}catch(Exception e){
			System.out.println("Error in insert Error data : "+ e.getMessage());
		}
	}
	
	public void executeBatch(List<String> batch) throws SQLException {		
		int count = 0;  
	    int successCount = 0;
	    int failCount = 0;
	    int notAavailable = 0;
		System.out.println("Batch Records Count = > "+ batch.size());
		
		Statement stmt = dbConn();
		try {				
			for(String query : batch){
				stmt.addBatch(query);
			}
			int count1[] = stmt.executeBatch();
			//System.out.println(" Number of record inserted in Batch => " + stmt.executeBatch());
			stmt.close();
			con.close();
			
		}catch (BatchUpdateException buex) {
			System.out.println("Something went wrong in executing Batch query : "+ buex.getMessage());		
			//stmt.close();
			//con.close();
			
			
	            buex.printStackTrace();
	           // LogUtil.error(buex);
	            int[] updateCounts = buex.getUpdateCounts();
	            System.out.println("Update records Count : "+ updateCounts.length);
	            for (int i = 0; i < updateCounts.length; i++) {
	            	
	            	System.out.println(" updateCounts["+i+"] : "+ updateCounts[i] +" , "+  Statement.SUCCESS_NO_INFO  +" , "+  Statement.SUCCESS_NO_INFO);
	                if (updateCounts[i] >= 0) {
	                    successCount++;	                         
	                } else if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
	                    notAavailable++;	                     
	                } else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
	                    failCount++;	                     
	                }
	            }
	            
	        } finally {
	        	System.out.println("Number of affected rows before Batch Error :: " + successCount);
	        	System.out.println("Number of affected rows not available:" + notAavailable);
	        	System.out.println("Failed Count in Batch because of Error:" + failCount);
	        	stmt.close();
				con.close(); 
	        }			
		
	}

	public boolean executeUniqueBatch(Set<String> batch , Set<String> batch_Json, String Table, String colsList) throws SQLException {		
		int count = 0;  
	    int successCount = 0;
	    int failCount = 0;
	    int notAavailable = 0;
	    
		System.out.println("Batch Records Count = > "+ batch.size());
		
		Statement stmt = dbConn();
		try {	
			
			for(String query : batch){
				stmt.addBatch(query);
				
			}
			stmt.executeBatch();
			//System.out.println(" Number of record inserted in Batch => " + stmt.executeBatch());
			batch.clear();
			stmt.close();
			con.close();
			
			
		}catch (BatchUpdateException buex) {
			///System.out.println("Duplicate re");		
			//stmt.close();
			//con.close();		
			
	            buex.printStackTrace();
	           // LogUtil.error(buex);
	            
	            int[] updateCounts = buex.getUpdateCounts();
	            
	            System.out.println("Update records Count after exception : "+ updateCounts.length);
//	            for (int i = 0; i < updateCounts.length; i++) {
//	            	
//	            	System.out.println(" updateCounts["+i+"] : "+ updateCounts[i] +" , "+  Statement.EXECUTE_FAILED  +" , "+  Statement.SUCCESS_NO_INFO);
//	                if (updateCounts[i] >= 0) {
//	                    successCount++;	                         
//	                } else if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
//	                    notAavailable++;	                     
//	                } else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
//	                    failCount++;	                     
//	                }
//	            }	            
	            
	            if(updateCounts.length != batch.size()){
		           List<String> dupSubList = new ArrayList(batch); 
		           //System.out.println("dupSubList : "+ dupSubList.size());
		           System.out.println("last insert statement : "+ dupSubList.get(updateCounts.length ));
		           
		           // insert duplicate records
		           List<String> dupRec = new ArrayList<String>();
		           if(batch.size() == 1){
		        	   	dupRec.add(dupSubList.get(updateCounts.length));
		        	   	System.out.println("1. Sub list size : "+ dupRec.size());
		           }
		           else{
		        	   if(updateCounts.length == 0)
		        		   dupRec.add(dupSubList.get(updateCounts.length));
		        	   else
		        		   dupRec.add(dupSubList.get(updateCounts.length + 1));
		        	   
		        	   System.out.println("2. Sub list size : "+ dupRec.size());
		           }
		           
		           
		           System.out.println("dupRec : "+ dupRec.size() +  "Query : "+ dupRec.get(0));
		           
		           String[] splitStr = dupRec.get(0).split(",");
		           Iterator iter = batch_Json.iterator();
		           String strtemp = null;
			   		while (iter.hasNext()) {
			   			strtemp = iter.next().toString();
			   		    //System.out.println(strtemp);			   		    
			   		    if(strtemp.toString().contains(splitStr[12])){
			   		    	//System.out.println("Found");
			   		    	break;
			   		    }
			   		}		   		
		           String query = "insert into " + Table + " (" + colsList + ")" + " values (" + splitStr[12] + ",'" + strtemp
							+ "', sysdate )";
		           dupRec.clear();
		           dupRec.add(query);
		           System.out.println("New Query : "+ dupRec.get(0));
		           executeBatchWithDuplicateData(dupRec);
		           
		           if(batch.size() > 1 ){
		        	   
		        	   if(updateCounts.length == 0) {
		        		   List<String> arr = dupSubList.subList(updateCounts.length + 1, batch.size());
		        		   Set<String> batch1 = new HashSet<String>(arr);
		        		   executeUniqueBatch(batch1 , batch_Json ,  Table,  colsList);
		        	   }else{
		        		   if(batch.size() == updateCounts.length + 2){
			        		   List<String> arr = dupSubList.subList(updateCounts.length + 1, batch.size());
			        		   Set<String> batch1 = new HashSet<String>(arr);
			        		   executeUniqueBatch(batch1 , batch_Json ,  Table,  colsList);
		        		   }else{
		        			   List<String> arr = dupSubList.subList(updateCounts.length + 2, batch.size());
			        		   Set<String> batch1 = new HashSet<String>(arr);
			        		   executeUniqueBatch(batch1 , batch_Json ,  Table,  colsList);
		        		   }
		        	   }
			           //List<String> arr = dupSubList.subList(updateCounts.length + 2, batch.size());
			          
			           
			           //display data for dubgging
//			           for(int i = updateCounts.length + 2; i <  batch1.size() ; i++){
//			        	   System.out.println("Rec Index : "+ i + ", data : "+ arr.get(i));
//			           }
		            
			           // Calling recursive function
			          
	            	}else{
	            		return true;
	            	}
	            }else{
	            	return true;
	            }
		}	
		if(batch.size() > 0)
			return false;
		else
			return true;
	}
	
	
	

	public void executeBatchProcedure(
			Object[] setAPPL_NAME ,
			Object[] setAppl_ID,
			Object[] setTRANSACTION_TYPE ,
			Object[] setSUB_TRX_TYPE,
			Object[] setCCO_USER_ID ,
			Object[] setINSTANCE_ID ,
			Object[] setCONTRACT_NUMBER ,
			Object[] setSERVICE_LINE_ID ,
			Object[] setSOURCE_CP_LINE_ID ,
			Object[] setTERMINATION_DATE ,
			Object[] setOFFSET ,
			Object[] setPARTITION
			 ) throws SQLException {		
			    
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 con = DriverManager.getConnection("jdbc:oracle:thin:@scan-nprd-2023:1541/DV1CSF_SRVC_OTH.cisco.com", "APPS", "P0t3ntial");
		 
		
		Array appl_name = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array appl_req_id = con.createArrayOf("fnd_table_of_number", setAppl_ID);
		Array tnx_type = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array subTnx_type = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array cco_usr_id = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array inst_num = con.createArrayOf("fnd_table_of_number", setAPPL_NAME);
		Array cont_num = con.createArrayOf("fnd_table_of_number", setAPPL_NAME);
		Array ser_ln_id = con.createArrayOf("fnd_table_of_number", setAPPL_NAME);
		Array src_cp_ln_id = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array term_date = con.createArrayOf("fnd_table_of_varchar2_255", setAPPL_NAME);
		Array offset = con.createArrayOf("fnd_table_of_number", setAPPL_NAME);
		Array partition = con.createArrayOf("fnd_table_of_number", setAPPL_NAME);		
		
		System.out.println("Batch Records Count = > "+ setAPPL_NAME.length);
		
		//Statement stmt = dbConn();
		try {	
			System.out.println("Calling procedure....!!!");
			CallableStatement callStmt = con.prepareCall("call Apps.kafkaBulkInsert(?,?,?,?,?,?,?,?,?,?,?,?)");
			callStmt.setArray(1, appl_name);
			callStmt.setArray(2, appl_req_id);
			callStmt.setArray(3, tnx_type);
			callStmt.setArray(4, subTnx_type);
			callStmt.setArray(5, cco_usr_id);
			callStmt.setArray(6, inst_num);
			callStmt.setArray(7, cont_num);
			callStmt.setArray(8, ser_ln_id);
			callStmt.setArray(9, appl_name);
			callStmt.setArray(10, src_cp_ln_id);
			callStmt.setArray(11, offset);
			callStmt.setArray(12, partition);
			
			callStmt.execute();
			
			
		}catch (SQLException buex) {
			System.out.println("Exception in executeBatchProcedure : "+ buex.getMessage());
		}	
		
	}
	
	
	public void executeBatchWithDuplicateData(List<String> batch) throws SQLException {		
		
		System.out.println("Inside executeBatchWithDuplicateData function , Batch Records Count = > "+ batch.size());
		
		Statement stmt = dbConn();
		try {				
			for(String query : batch){
				stmt.addBatch(query);
			}	
			stmt.executeBatch();
			// System.out.println(" Number of record inserted in Batch => " + stmt.executeBatch());
			stmt.close();
			con.close();
			
		}catch (BatchUpdateException buex) {
			//flag = true;
			System.out.println("Something went wrong in executing Batch query : "+ buex.getMessage());		
			
		}
		// this will control if there is duplicate records
		
	}

	
	public void copyTableData(String srctable, String trgTable, String Errtable){
		Statement stmt = dbConn();
		try{
			System.out.println("Copy data from backup to main table.");
			String query = "insert into "+trgTable+"  (APPL_REQUEST_ID, APPL_NAME, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID,"
					+ " CONTRACT_NUMBER, SERVICE_LINE_ID, SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number)"+
            "select   APPL_REQUEST_ID,APPL_NAME, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID, CONTRACT_NUMBER, SERVICE_LINE_ID, "
            + "SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number from (select  row_number() over  ( partition by appl_request_id order by offset) rn,"
            + "appl_request_id rq_id, APPL_NAME, APPL_REQUEST_ID, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID, CONTRACT_NUMBER, SERVICE_LINE_ID, "
            + "SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number from "+srctable+" ) where rn = 1";
			
			//System.out.println("Copy Table Data Query :"+ query);
			stmt.execute(query);
			con.commit();
			
		}catch(Exception e){
			System.err.println("Exception in copy date to main table : "+ e.getMessage());
		}
		
		try{
			String query1 ="insert into "+Errtable+"  (APPL_REQUEST_ID, APPL_NAME, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID,"
					+ " CONTRACT_NUMBER, SERVICE_LINE_ID, SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number)"+
		            "select   APPL_REQUEST_ID,APPL_NAME, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID, CONTRACT_NUMBER, SERVICE_LINE_ID, "
		            + "SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number from (select  row_number() over  ( partition by appl_request_id order by offset) rn,"
		            + "appl_request_id rq_id, APPL_NAME, APPL_REQUEST_ID, TRANSACTION_TYPE, SUB_TRX_TYPE, CCO_USER_ID,INSTANCE_ID, CONTRACT_NUMBER, SERVICE_LINE_ID, "
		            + "SOURCE_CP_LINE_ID,TERMINATION_DATE,OFFSET ,partition_number from "+srctable+" ) where rn = 2";
			stmt.execute(query1);
			con.commit();
			
			stmt.close();
			
		}catch(Exception e){
			System.err.println("Exception in copy date to Duplicate data table : "+ e.getMessage());
		}
		
	}

	public void deleteTableData(String tabelName)
	{
		try{
			Statement stmt = dbConn();
			stmt.execute("delete from "+ tabelName);
			con.commit();
			stmt.close();
			
		}catch(Exception e){
			
		}
	}
	public void updateCommitedOffset(Map<Integer, Integer> hm, String ccwCommitTable) throws SQLException {
		try {
			Statement stmt = dbConn();
			for (Map.Entry e : hm.entrySet()) {
				String offsetCommitQuery = "update "+ccwCommitTable+" set OFFSET =" + e.getValue()
						+ " where partition_number = " + e.getKey();
				// System.out.println("Offset Query : "+ offsetCommitQuery);
				if (!Strings.isNullOrEmpty(offsetCommitQuery)) {
					stmt.execute(offsetCommitQuery);
					con.commit();
				}
			}
			con.close();
		} catch (Exception e) {
			System.out.println("updateCommitedOffset Exception : " + e.getMessage());
			con.close();
		}
	}
	
	public void updateNewOffset(Map<Integer, Integer> hm, String ccwCommitTable, String topicName, String group) throws SQLException {
		try {
			Statement stmt = dbConn();
			System.out.println("inside updateNewOffset function ");
			String query = "select * from "+ ccwCommitTable;
			ResultSet rs = stmt.executeQuery(query);
			boolean flag = false;
			for (Map.Entry e : hm.entrySet()){				
				//System.out.println("*************************************");
				while(rs.next()){					
					//System.out.println("Rs :"+ rs.getInt(1) +   " hm : "+ e.getKey());					
					if(Integer.parseInt(e.getKey().toString()) == rs.getInt(1)){
						flag = true;
						break;
					}				
				}				
				if(flag){
					String offsetCommitQuery = "update "+ccwCommitTable+" set OFFSET =" + e.getValue()
							+ " where partition_number = " + e.getKey();						
					
					if (!Strings.isNullOrEmpty(offsetCommitQuery)) {
						stmt.execute(offsetCommitQuery);
						con.commit();
					}						
				}
				else{
					insertCommitedOffset(Integer.parseInt(e.getKey().toString()), Integer.parseInt(e.getValue().toString()), ccwCommitTable,topicName,  group);
				}
				
			}
			//con.close();
		} catch (Exception e) {
			System.out.println("updateNewOffset Exception : " + e.getMessage());
			con.close();
		}
	}

	public void updateBeginningOffset(long l, int partition_Number, String ccwCommitTable) throws SQLException {
		try {
			Statement stmt = dbConn();

			String offsetCommitQuery = "update "+ccwCommitTable+" set OFFSET =" + l
					+ " where partition_number = " + partition_Number;
			// System.out.println("Offset Query : "+ offsetCommitQuery);
			if (!Strings.isNullOrEmpty(offsetCommitQuery)) {
				stmt.execute(offsetCommitQuery);
				con.commit();
			}
			con.close();
		} catch (Exception e) {
			System.out.println("insertCommitedOffset Exception : " + e.getMessage());
			con.close();
		}
	}

	public void insertCommitedOffset(int part_num, long intOffset, String ccwCommitTable,String topicName, String groupId) throws SQLException {
		try {
			Statement stmt = dbConn();
			String offsetCommitQuery = /*" insert into "+ccwCommitTable+" (Partition_number, offset,topic,consumer) values ("
					+ part_num + "," + intOffset + ","+ topicName + "," + groupId + ")";
*/
			
			"insert into "+ccwCommitTable+"(Partition_number, offset,topic,consumer) values ("+ part_num + "," + intOffset + ",'"+ topicName + "','" + groupId + "')";
			 System.out.println("Offset Query : "+ offsetCommitQuery);
			if (!Strings.isNullOrEmpty(offsetCommitQuery)) {
				stmt.execute(offsetCommitQuery);
				con.commit();
				con.close();
			}
		} catch (Exception e) {
			System.out.println("insertCommitedOffset Exception : " + e.getMessage());
			con.close();
		}
	}

}
