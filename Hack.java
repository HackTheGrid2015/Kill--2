package Experiment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Hack {
	// JDBC driver name and database URL
		static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static final String DB_URL = "jdbc:mysql://localhost:3306/HackTheGrid";

		// Database credentials
		static final String USER = "root";
		static final String PASS = "";
		static int count = 0;
		static int countMAX = 5000;
		static int fileNumber = 1;
		static File logger = new File("log.txt");

		
		public static void main(String args[]) {
		 	Connection conn = null;
		    Statement stmt = null;
		try {
			if (!logger.exists()) {
				logger.createNewFile();
			}
			
			Class.forName("com.mysql.jdbc.Driver");
	       
			System.out.println("\nConnecting to database...");
	        conn = DriverManager.getConnection(DB_URL, USER, PASS);
	        System.out.println("MySQL connection successfull");
			
	        File dir = new File("./data/");
	        File[] filesList = dir.listFiles();
	        
	        for (int fileId = 0; fileId < filesList.length; fileId++) {
	        	
	        	System.out.println("In file (" + fileNumber + ") " + filesList[fileId].getName() + ".");
	        	
	        	if (filesList[fileId].getName().equalsIgnoreCase(".DS_Store")) {
	        		System.out.println("Ignore file " + filesList[fileId].getName() + ".");
	        		continue;
	        	}
	        	
	        	BufferedReader br = new BufferedReader(new FileReader(filesList[fileId].getAbsoluteFile()));
	        	System.out.println("Buffered file " + filesList[fileId].getName() + ".");
	        	String line;
				Boolean flag = true;
				row r = new row();
				String sql1 = "INSERT INTO data (siteid,meterid,dttm,demand_kWh) values (?,?,?,?) ";
				PreparedStatement preparedStatement = conn.prepareStatement(sql1);
				count = 0;
				int times = 0;
				while ((line = br.readLine()) != null) {
					if (flag) {
						flag = false;
						continue;
					}
					else {
						if (line != null || !line.equals("")) {
							String[] splits = line.split(",");
							if(splits[0] != null || !splits[0].isEmpty()) {
								r.siteid = splits[0].substring(1,splits[0].length()-1);
							}
							if(splits[1] != null || !splits[1].isEmpty()) {
								r.meterid = splits[1].substring(1,splits[1].length()-1);
							}
							if(splits[3] != null || !splits[3].isEmpty()) {
								r.dem = convertFloat(splits[3]);
							}
							if(splits[2] != null || !splits[2].isEmpty()) {
								SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								Date d = f.parse(splits[2].substring(1,splits[2].length()-1));
								r.date = d;
							}
							
							preparedStatement.setString(1, r.siteid);
							preparedStatement.setString(2, r.meterid);
							preparedStatement.setTimestamp(3, new java.sql.Timestamp(r.date.getTime()));
							preparedStatement.setFloat(4, r.dem);
							preparedStatement.addBatch();
							
							if (count++ > countMAX) {
								count = 0;
								times++;
								preparedStatement.executeBatch();
								preparedStatement.close();
								System.out.println("Inserted " + filesList[fileId].getName() + " -> rows " + (times-1)*countMAX + " to " + times*countMAX + ".");
								preparedStatement = conn.prepareStatement(sql1);
							}
						}
					}
				} 
				preparedStatement.executeBatch();
				br.close();
				System.out.println("Out file (" + (fileNumber++) + ") " + filesList[fileId].getName() + ".");
	        }
	        
		} catch (IOException | ClassNotFoundException | SQLException | ParseException e ) {
			e.printStackTrace();
		}
	}
		
	public static void log (String str) {
		try {
			FileWriter fw = new FileWriter(logger.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			bw.write(sdf.format(new Date()) + " :" + str + "\n");
			bw.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static float convertFloat(String str){  
		float d;
		try  
		  {  
		     d = Float.parseFloat(str);  
		  }  
		  catch(NumberFormatException nfe)  
		  {  
		    return 0F;
		  }  
		  return d;  
	}

	public static class row {
		String siteid;
		String meterid;
		Date date;
		Float dem;
	}
}
