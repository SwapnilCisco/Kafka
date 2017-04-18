package functionalUtility;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Collection;

import com.sun.java_cup.internal.runtime.Scanner;
import com.utility.dbConnection;

public class Test {

	public static void main(String[] args) {
		
		 
	    String str[][]=new String[10][3];
	    int j = 0;
	    int req_id = 1000;
	    String strI = "{APPL_REQ_ID :"+req_id+"}";
	    String date  = "3/5/2017";	    
	    
		for(int i = 0;i<10;i++)		{
			str[i][j++] = req_id+"";
			str[i][j++] = strI;
			str[i][j++] = date;
			req_id++;
			j = 0;
		}
		
//		for(int i = 0 ; i <  10 ;  i++){
//			j = 0;
//			System.out.println(i +" => "+ str[i][j++] +  str[i][j++] + str[i][j++]);
//		}
	    
	     
		SimpleDateFormat formatter1 = new SimpleDateFormat("MM/dd/yyyy");
		
	       
	    Connection conn = null; 
		CallableStatement callableStatement = null; 
		String proc = "{call insertEMPLOYEE(?,?)}"; 
		try{	//get 
			Class.forName("oracle.jdbc.driver.OracleDriver");
			 conn = DriverManager
					.getConnection("jdbc:oracle:thin:@scan-nprd-2023:1541/DV1CSF_SRVC_OTH.cisco.com", "APPS", "P0t3ntial");
			//create callableStatement c
			for(int i = 0;i<10;i++)	{
				 callableStatement = conn.prepareCall(proc);   
			     callableStatement.setInt(1, Integer.parseInt(str[i][0])); 
			     callableStatement.setString(2, str[i][1]); 
			     //callableStatement.setString(3,"to_date("+date1+"','MM/dd/YYYY')"); 
			     //callableStatement.getda
			     callableStatement.addBatch();
			     callableStatement.executeBatch();
				}
			
			
			conn.commit();
			conn.close();   
			System.out.println("Records inserted successfully.");
			}catch(Exception e)
		{ 
				e.printStackTrace();
		}
		
		
		
	}
	

}
