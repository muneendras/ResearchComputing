package com.jdbc.oracle;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OracleMain {

	public static void main(String args[]) throws ClassNotFoundException, SQLException {
		final String secretKey = "yxZhIyFacDxYri6WwQftIVVksDRYpqrQ";
		Connection connection = null;
		OracleImpl hrImpl = new OracleImpl();
		Map<String, String> conProps = new HashMap<String, String>();

		try {
			// opening file for reading in Java
			FileInputStream file = new FileInputStream(
					"/home/msriram/IAMconnector/conn.properties");
			Properties prop = new Properties();
			prop.load(file);			
			String usrName = CredsEncrypt.decrypt(prop.getProperty("PARAM_ADMIN_USER"), secretKey) ;
			String pass = CredsEncrypt.decrypt(prop.getProperty("PARAM_ADMIN_PASS"), secretKey) ;	
			String crUserPass = CredsEncrypt.decrypt(prop.getProperty("PARAM_CR_ACC_PASS"), secretKey) ;
			//System.out.println(usrName);
			//System.out.println(pass);
			//System.out.println(crUserPass);
			Map<String, String> settings = new HashMap<String, String>();
			Map<String, Object> params = new HashMap<String, Object>();
			String dbURL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME=XE)))";
			//System.out.println("jdbcurl=" + dbURL);
			settings.put("PARAM_ADMIN_USER", new String(usrName));
			settings.put("PARAM_ADMIN_PASS", new String(pass));			
			settings.put("PARAM_JDBC_URL", new String("jdbc:oracle:thin:@localhost:1521:xe"));
			settings.put("DRIVER_CLASS_NAME", new String("oracle.jdbc.driver.OracleDriver"));
			//System.out.println("Oracle Localhost =" + settings);
			Map<String, Map<String, String>> reqItems = hrImpl.getReqItems(settings);
			List<String> reqItemsList = new ArrayList<String>();
			reqItemsList = hrImpl.getReqItemsList(settings);
			System.out.println("reqItems ## " + reqItems);

			for (String rItem : reqItemsList) {
				//System.out.println("Name @@## " + reqItems.get(rItem).get("OPERATION_TYPE"));
				String operType = reqItems.get(rItem).get("OPERATION_TYPE");
				String userName = reqItems.get(rItem).get("USER_ID");
				String ent = reqItems.get(rItem).get("ENTITLEMENT");
				String uid = reqItems.get(rItem).get("UID");

				if (operType.equals("Create")) {
					/* Create an Account */
					params.put("UserName", new String(userName));
					params.put("Password", new String(crUserPass));
					params.put("ENTITLEMENT", new String(ent));
					params.put("UID", new String(uid));
					hrImpl.createAccount(settings, params);
				} else if (operType.equals("Add")) {
					/* Add an entitlement to Account */
					params.put("UserName", new String(userName));
					params.put("ENTITLEMENT", new String(ent));
					params.put("UID", new String(uid));
					hrImpl.addAppRoleToAccount(settings, params);
				} else if (operType.equals("Revoke")) {
					/* Revoke entitlement from an Account */
					params.put("UserName", new String(userName));
					params.put("ENTITLEMENT", new String(ent));
					params.put("UID", new String(uid));
					hrImpl.removeAppRoleFromAccount(settings, params);
				} else if (operType.equals("Delete")) {
					/* Delete an Account */
					params.put("UserName", new String(userName));
					params.put("UID", new String(uid));
					hrImpl.deleteAccount(settings, params);
				} else if (operType.equals("Enable")) {
					/* Enable an Account */
					params.put("UserName", new String(userName));
					params.put("UID", new String(uid));
					connection = hrImpl.getConnection(settings);
					hrImpl.enableAccount(settings, params);
				} else if (operType.equals("Disable")) {
					/* Enable an Account */
					params.put("UserName", new String(userName));
					params.put("UID", new String(uid));
					connection = hrImpl.getConnection(settings);
					hrImpl.disableAccount(settings, params);
				}

			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}

	}
}
