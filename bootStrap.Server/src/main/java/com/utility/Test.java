package com.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import CumsumerClasses.AmpCounsumerRespBean;

public class Test {

	public static void main(String[] args) throws ParseException {
		
		/*
		String formatedDate = null;
		
		String strDate = "2017-03-29";
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
		
		System.out.println("Date : "+formatedDate);
		
		
		
		//===================================================
		String strda = "2017-03-29 11:40:55 AM";
		//SimpleDateFormat strDate1 =  new SimpleDateFormat("yyyy-MM-dd");// = new Date("2017-03-29");
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date strDate1 = (Date)formatter1.parse(strda);
		
		System.out.println("Date : "+ strDate1);

		String strDate = strDate1.toString();
		System.out.println("Date 1 : "+ strDate + ", Length : "+strDate.length());
        SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
        Date dateStr = formatter.parse(strDate);
        String formattedDate = formatter.format(dateStr);
        System.out.println("yyyy-MM-dd date is ==>"+formattedDate);
        
        Date date1 = formatter.parse(formattedDate);
        formatter = new SimpleDateFormat("MM/dd/yyyy");
        formattedDate = formatter.format(date1);
        System.out.println("dd-MMM-yyyy date is ==>"+formattedDate);
        
        */
        //================================================================
		
		//String str = "12345,56576,67686,675446,698678564,12345,56576,67686";
		String[] str1 = new String[15];// str.split(",");
		str1[0] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2459\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[1] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2451\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[2] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2452\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[3] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2453\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[4] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2459\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[5] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2457\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[6] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2458\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[7] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2459\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[8] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2457\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[9] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2458\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[10] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2459\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[11] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2457\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[12] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2458\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[13] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2467\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		str1[14] = "{\"APPL_NAME\":\"AMP\",\"APPL_REQUEST_ID\":\"2468\",\"CCO_USER_ID\":\"admdbryson\",\"CONTRACT_NUMBER\":\"1505774\"}";
		
		Set<String> uniqueAppReqIds = new HashSet<String>();
		List<String> dupData = new ArrayList<String>();
		int size = 0;
		for(int i = 0; i < str1.length  ; i++){
			System.out.println("Data : "+ str1[i]);
			uniqueAppReqIds.add(str1[i]);
			if(size == uniqueAppReqIds.size())
				dupData.add(str1[i]);
			
			size = uniqueAppReqIds.size();
	}

		System.out.println("Unique Length : "+ uniqueAppReqIds.size());
		System.out.println("Dup Length : "+ dupData.size());
		
	}

	
	
	
	public void getUniqueAmpCounsumerRespBean(List<AmpCounsumerRespBean> commObjList){
		HashMap<String, List<AmpCounsumerRespBean>> uniRecs = new HashMap<String, List<AmpCounsumerRespBean>>();
		HashMap<String, List<AmpCounsumerRespBean>> dupRecs = new HashMap<String, List<AmpCounsumerRespBean>>();
		List<AmpCounsumerRespBean> uniqueUtitlityObj = new ArrayList<AmpCounsumerRespBean>();
		List<AmpCounsumerRespBean> duplicateAmpCounsumerRespBean = new ArrayList<AmpCounsumerRespBean>();
		Set<Long> uniqueAppReqIds = new HashSet<Long>();
		for(AmpCounsumerRespBean commObj : commObjList){
			if(!uniqueAppReqIds.contains((Long)commObj.getAppReqId())){
				uniqueUtitlityObj.add(commObj);
			}else{
				duplicateAmpCounsumerRespBean.add(commObj);
			}
		}
		uniRecs.put("uniqueAmpCounsumerRespBean", uniqueUtitlityObj);
		dupRecs.put("duplicateAmpCounsumerRespBean", duplicateAmpCounsumerRespBean);
		//return result;
	}


}
