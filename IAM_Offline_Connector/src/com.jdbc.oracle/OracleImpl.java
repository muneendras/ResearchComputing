package com.jdbc.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aveksa.afx.common.connector.service.JavaCodeBasedConnectorBase;
import com.aveksa.afx.common.connector.service.data.JCBCProperty;
import com.aveksa.afx.common.connector.service.data.JCBCPropertyType;
import com.aveksa.afx.common.connector.service.response.JCBCStatus;

/**
 * Java Connector implementation for Oracle Database.
 * @author Muneendra Sriram
 */
public class OracleImpl   {		
	
	private class ReqItemList extends ArrayList<Map<String,String>>{};
	Connection connection = null;
	
	public void createAccount(Map<String, String> settings, Map<String, Object> params) throws Exception {
		String username = (String) params.get("UserName");
		String password = (String) params.get("Password");
		String uid = (String) params.get("UID");
		String query = String.format("CREATE USER \"%s\" IDENTIFIED BY \"%s\"", username, password);
		System.out.println("query # "+query);
		executeQuery(settings, query,uid);
		System.out.println("Account : "+ username +" has been created succesfully.");
		addAppRoleToAccount(settings,params);
		
	}
	public void addAppRoleToAccount(Map<String, String> settings, Map<String, Object> params) throws Exception {
		String username = (String) params.get("UserName");
		String ent = (String) params.get("ENTITLEMENT");
		String uid = (String) params.get("UID");
		String query = String.format("GRANT \"%s\" TO \"%s\"", ent, username);
		System.out.println("query # "+query);
		executeQuery(settings, query,uid);
		System.out.println("ENTITLEMENT : "+ent +" has been granted to "+username +" succesfully.");		
	}
	
	public void removeAppRoleFromAccount(Map<String, String> settings, Map<String, Object> params) throws Exception {
		String username = (String) params.get("UserName");
		String ent = (String) params.get("ENTITLEMENT");
		String uid = (String) params.get("UID");
		String query = String.format("REVOKE \"%s\" FROM \"%s\"", ent, username);
		System.out.println("query # "+query);
		executeQuery(settings, query,uid);
		System.out.println("ENTITLEMENT : "+ent +" has been revoked to "+username +" succesfully.");
		
	}
	
	
	public void deleteAccount(Map<String, String> settings, Map<String, Object> params) throws Exception  {
		String username = (String) params.get("UserName");
		String uid = (String) params.get("UID");
		String query = String.format("DROP USER \"%s\" CASCADE", username);
		executeQuery(settings, query,uid);
		System.out.println("Account "+username +" has been deleted succesfully.");
	}

	
	public void enableAccount(Map<String, String> settings, Map<String, Object> params) throws Exception  {
		String username = (String) params.get("UserName");
		String uid = (String) params.get("UID");
		String query = String.format("GRANT CREATE SESSION TO \"%s\"", username);
		executeQuery(settings, query,uid);
		System.out.println("Account "+username +" has been enabled succesfully.");
		
	}

	public void disableAccount(Map<String, String> settings, Map<String, Object> params) throws Exception  {
		String username = (String) params.get("UserName");
		String uid = (String) params.get("UID");
		String query = String.format("REVOKE CREATE SESSION FROM \"%s\"", username);
		executeQuery(settings, query,uid);
		System.out.println("Account "+username +" has been disabled succesfully.");
		
	}
	
	public Connection getConnection(Map<String, String> settings)
			throws ClassNotFoundException, SQLException {
		//System.out.println("settings =" + settings);
		//System.out.println("settings =" + settings.get("DRIVER_CLASS_NAME"));
		String username = settings.get("PARAM_ADMIN_USER");
		String password = settings.get("PARAM_ADMIN_PASS");
		String url = settings.get("PARAM_JDBC_URL");	
		String driverClass = settings.get("DRIVER_CLASS_NAME");
		Class.forName(driverClass);
		return DriverManager.getConnection(url, username, password);
	}

	private void executeQuery(Map<String, String> settings, String query,String uid) throws ClassNotFoundException, SQLException {
		
		try {
			
			connection = getConnection(settings);
			//System.out.println("connection =" + connection);
			//System.out.println("query =" + query);
			Statement stmt=connection.createStatement();
			ResultSet rs=stmt.executeQuery(query);	
			String return_code = "0";
			String return_msg = "Success";			
			PreparedStatement preStmt = connection.prepareStatement("UPDATE HR_STG_TABLE_PROV_DATA SET \"STATUS_CODE\" = ?,\"STATUS_MSG\" = ? where \"UID\" = ?");
			preStmt.setString(1, return_code);
			preStmt.setString(2, return_msg);
			preStmt.setString(3, uid);
			//ResultSet rsUpdate=stmt.executeQuery(queryUpdate);
			preStmt.executeUpdate();
			preStmt.close();
			System.out.println("Status updated to statging table succesfully.");
		} 
		catch (SQLException e){			
			System.out.println("SQLException Message =" + e.getMessage());
			System.out.println("SQLException code =" + e.getErrorCode());
			String return_code = String.valueOf(e.getErrorCode());
			String return_msg = e.getMessage();
			PreparedStatement preStmt = connection.prepareStatement("UPDATE HR_STG_TABLE_PROV_DATA SET \"STATUS_CODE\" = ?,\"STATUS_MSG\" = ? where \"UID\" = ?");
			preStmt.setString(1, return_code);
			preStmt.setString(2, return_msg);
			preStmt.setString(3, uid);
			preStmt.executeUpdate();
			preStmt.close();
			
			System.out.println("Status updated to statging table succesfully.");			
		}
		finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception localException) {
					
					System.out.println("localException =" + localException);
					
				}
			}
		}
	}
	
	protected Map<String,Map<String,String>> getReqItems(Map<String, String> settings)
			throws SQLException, ClassNotFoundException {
		Map<String,Map<String,String>> ret = new HashMap<String,Map<String,String>>();		
		final String REQID_ITEMS_QUERY = "select * from HR_STG_TABLE_PROV_DATA where status_code is null";
		ResultSet rs = null;
		connection = getConnection(settings);
		if (connection != null) {
			try {
				String query = REQID_ITEMS_QUERY;
				System.out.println("getReqItems # query # " + query);
				PreparedStatement pstmt = connection.prepareStatement(query);
				rs = pstmt.executeQuery();
				ret = setReqItems(rs);
			} finally {
				if (rs != null) {
					rs.close();
				}
			}
		}
		//System.out.println("Request Items	#");
		return ret;
	}
	
	private Map<String,Map<String,String>> setReqItems(ResultSet rs) throws SQLException {
		
		Map<String,Map<String,String>> ret = new HashMap<String,Map<String,String>>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount(); 
		//System.out.println ("SetReqItems # ");		
		while (rs.next()) {		
				String uid = rs.getString("UID");				
				Map<String, String> dbRow = new HashMap<String, String>();
				for (int i = 1; i <= columnCount; i++) {					
					String colName = rsmd.getColumnName (i); 
					//System.out.println ("SetReqItems # "+ colName);
					String val = rs.getString (colName);
					//System.out.println ("SetReqItems val # "+ val);						
					dbRow.put (colName, val);					
				}
				ret.put(uid,dbRow);
				//System.out.println("ril  #"+ret);
		}
		
		return ret;
		
	}
	
	
	protected List<String>  getReqItemsList(Map<String, String> settings)
			throws SQLException, ClassNotFoundException {
		List<String> reqList = new ArrayList<String>();		
		final String REQID_ITEMS_QUERY = "select * from HR_STG_TABLE_PROV_DATA where status_code is null";
		ResultSet rs = null;
		connection = getConnection(settings);
		if (connection != null) {
			try {
				String query = REQID_ITEMS_QUERY;
				//System.out.println("getReqItems # query # " + query);
				PreparedStatement pstmt = connection.prepareStatement(query);
				rs = pstmt.executeQuery();
				reqList = setReqItemsList(rs);
			} finally {
				if (rs != null) {
					rs.close();
				}
			}
		}
		//System.out.println("Request Items	#");
		return reqList;
	}
	
	private List<String> setReqItemsList(ResultSet rs) throws SQLException {
		
		List<String> reqList = new ArrayList<String>();
		List<String> reqListRet = new ArrayList<String>();				
		while (rs.next()) {									
					String val = rs.getString ("UID");										
					reqList.add(val);	
						}
		reqListRet.addAll(reqList);
		//System.out.println("reqListRet  #"+reqListRet);		
		return reqListRet;
		
	}
			 
}

