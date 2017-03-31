package com.utility;

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import joptsimple.internal.Strings;

public class commonUtility {
	
	public String readPropKey(String key){
		Properties prop = null;
		try{
			
			String Path = new File("Config.properties").getAbsolutePath();
			FileInputStream fis = new FileInputStream(new File(Path));//"C:\\Users\\Administrator\\workspace\\bootStrap.Server\\Config.properties"));
			prop = new Properties();
			prop.load(fis);
			return prop.getProperty(key);
		}catch(Exception e){
			System.out.println("Exception in reading Config.Propeties file");
			return null;
		}
		
	}
	
	public Properties readProp(){
		Properties prop = null;
		try{
			String Path = new File("Config.properties").getAbsolutePath();
			FileInputStream fis = new FileInputStream(new File(Path));//"C:\\Users\\Administrator\\workspace\\bootStrap.Server\\Config.properties"));
			prop = new Properties();
			prop.load(fis);
			return prop;
		}catch(Exception e){
			System.out.println("Exception in reading Config.Propeties file");
			return null;
		}
		
	}


	
	public String dateFormat(Date strDate){
		String formatedDate = null;
		//System.out.println("Date : "+ strDate);
		try{
			
			if(strDate == null){
				//System.out.println("2");
				return "null";			
			}else{
				
				String dateStr = strDate.toString(); // "Fri Feb 17 21:55:07 PST 2017";
				DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
				Date date;
				
				date = (Date)formatter.parse(dateStr);					
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				String timeZ = null;
				// Calculate PM_AM
				if((cal.get(Calendar.HOUR)) > 12)
					timeZ = "AM";
				else
					timeZ = "PM";
				//Arrange date format as per Requirement
				formatedDate = (cal.get(Calendar.MONTH) + 1) +"/"+ cal.get(Calendar.DATE) +"/" + cal.get(Calendar.YEAR)
				+" "+ (cal.get(Calendar.HOUR))+":"+ (cal.get(Calendar.MINUTE))+":"+(cal.get(Calendar.SECOND));
				
				return "to_date('"+formatedDate+"','MM/dd/YYYY HH24:MI:SS')";
			}
		}catch(Exception e){			
			System.out.println("Exception in dateFormat() function || Date : "+strDate +" || Error : "+ e.getMessage());
			return null;
		}
		
	}
	
	public String toStringFormat(String strValue){
		
		try{
			//System.out.println("Inside toStringFormat() function, String Vaule : "+ strValue + ", Flag : " +Strings.isNullOrEmpty(strValue));
			if(Strings.isNullOrEmpty(strValue)){				
				return null;			
			}else{				
				return "'"+strValue+"'";				
			}
		}catch(Exception e){
			System.out.println("Exception in toStringFormat() function || Value : "+strValue);
			return null;
		}		
	}


}
