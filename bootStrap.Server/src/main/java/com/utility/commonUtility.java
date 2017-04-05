package com.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
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
			
			String Path = new File("src//main//resources//Config.properties").getAbsolutePath();
			FileInputStream fis = new FileInputStream(new File(Path));//"C:\\Users\\Administrator\\workspace\\bootStrap.Server\\Config.properties"));
			//InputStream fis = commonUtility.class.getClassLoader().getResourceAsStream("client.properties");			
			prop = new Properties();
			prop.load(fis);
			return prop.getProperty(key);
		}catch(Exception e){
			System.out.println("Exception in reading Config.Propeties file" + e.getMessage());
			return null;	
		}
		
	}
	
	public Properties readProp(){
		Properties prop = null;
		try{
			String Path = new File("src//main//resources//Config.properties").getAbsolutePath();
			FileInputStream fis = new FileInputStream(new File(Path));//"C:\\Users\\Administrator\\workspace\\bootStrap.Server\\Config.properties"));
			//InputStream fis = commonUtility.class.getClassLoader().getResourceAsStream("client.properties");
			prop = new Properties();
			prop.load(fis);
			return prop;
		}catch(Exception e){
			System.out.println("Config.properties file not located on src//main//resources//Config.properties, hence reading from local file");
			String Path = new File("Config.properties").getAbsolutePath();
			FileInputStream fis;
			try {
				fis = new FileInputStream(new File(Path));
				prop = new Properties();
				prop.load(fis);
				return prop;
			} catch (Exception e1) {				
				e1.printStackTrace();
				return null;
			}
			
		}
		
	}


	
	public String dateFormat(String strDate){
		String formatedDate = null;
		
		
		try{
			
			if(strDate == null){
				//System.out.println("2");
				return "null";			
			}else if(strDate.toString().length() == 10){
				//System.out.println("Date "+ strDate);
				//System.out.println("Date length : "+ strDate.toString().length());
				
				/*
				String strda = strDate.toString();
				SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
				Date Date1 = (Date)formatter1.parse(strda);
				
				String strDate1 = Date1.toString();
				SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
		        Date dateStr = formatter.parse(strDate1.toString());
		        String formattedDate = formatter.format(dateStr);
				
				Date date1 = formatter.parse(formattedDate);
		        formatter = new SimpleDateFormat("MM/dd/yyyy");
		        formattedDate = formatter.format(date1);
		        return "to_date('"+formatedDate+"','MM/dd/YYYY')";
		        */
				//System.out.println("Date Format is yyyy-MM-dd");
				
		        String bdate = strDate.toString();
		        //System.out.println("Before Date : "+ bdate);
		        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		        Date adate=sdf.parse(bdate);
		        sdf=new SimpleDateFormat("MM/dd/yyyy");
		       // System.out.println("After Date : "+ sdf.format(adate));
		        formatedDate = sdf.format(adate);
		        return "to_date('"+formatedDate+"','MM/dd/YYYY')";
		        		
			}else{
				//System.out.println("Date : "+ strDate);
				//System.out.println("Date length : "+ strDate.toString().length());
				
				/*
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
				*/				
				
				String strDate1 = strDate.toString();
				SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
		        Date dateStr = formatter.parse(strDate1.toString());
		        String formattedDate = formatter.format(dateStr);
				
				Date date1 = formatter.parse(formattedDate);
		        formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		        formattedDate = formatter.format(date1);
				
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
