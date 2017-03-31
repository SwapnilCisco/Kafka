package CumsumerClasses;

import java.util.Date;

public class AmpCounsumerRespBean {
	

	String APPL_NAME;
	long APPL_REQUEST_ID;
	String TRANSACTION_TYPE;
	String SUB_TRX_TYPE;
	String CCO_USER_ID;	
	long INSTANCE_ID;
	long CONTRACT_NUMBER;
	long SERVICE_LINE_ID;
	String SOURCE_CP_LINE_ID;
	Date TERMINATION_DATE;
		
	
	public String getApplName() {
		return APPL_NAME;
	}
	public void setApplName(String APPL_NAME) {  
		this.APPL_NAME = APPL_NAME;
	}
	public long getAppReqId() {
		return APPL_REQUEST_ID;
	}
	public void setAppReqId(int APPL_REQUEST_ID) {
		this.APPL_REQUEST_ID = APPL_REQUEST_ID;
	}
	public String getTrnxType() {
		return TRANSACTION_TYPE;
	}
	public void setTrnxType(String TRANSACTION_TYPE) {
		this.TRANSACTION_TYPE = TRANSACTION_TYPE;
	}
	
	public String getSubTnxType() {
		return SUB_TRX_TYPE;
	}
	public void setSubTnxType(String SUB_TRX_TYPE) {
		this.SUB_TRX_TYPE = SUB_TRX_TYPE;
	}
	
	public String getCCOUserId() {
		return CCO_USER_ID;
	}
	public void setCCOUserId(String CCO_USER_ID) {
		this.CCO_USER_ID = CCO_USER_ID;
	}
	
	public long getInstanceId() {
		return INSTANCE_ID;
	}
	public void setInstanceID(int INSTANCE_ID) {
		this.INSTANCE_ID = INSTANCE_ID;
	}
	
	
	public long getContractNumber() {
		return CONTRACT_NUMBER;
	}
	public void setContractNumber(int CONTRACT_NUMBER) {
		this.CONTRACT_NUMBER = CONTRACT_NUMBER;
	}
	
	public long getSerLineId() {
		return SERVICE_LINE_ID;
	}
	public void setSerLineId(long SERVICE_LINE_ID) {
		this.SERVICE_LINE_ID = SERVICE_LINE_ID;
	}
	
	public String getSrcCpLineId() {
		return SOURCE_CP_LINE_ID;
	}
	public void setSrcCpLineId(String SOURCE_CP_LINE_ID) {
		this.SOURCE_CP_LINE_ID = SOURCE_CP_LINE_ID;
	}
	
	
	public Date getTerDate() {
		return TERMINATION_DATE;
	}
	public void setTerDate(Date TERMINATION_DATE) {
		this.TERMINATION_DATE = TERMINATION_DATE;
	}
}
