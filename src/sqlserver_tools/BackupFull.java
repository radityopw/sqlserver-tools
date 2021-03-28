package sqlserver_tools;

import java.util.logging.*;
import java.util.*;
import java.io.*;
import java.sql.*;
import java.nio.file.*;

public class BackupFull{
	
	private static final Logger LOGGER = Logger.getLogger( BackupFull.class.getName() );
	
	public static void main(String[] a) throws Exception{
		
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new SimpleFormatter());
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
				
		Properties prop = new Properties();
		String fileName = "app.config";
		InputStream is = null;
		File backupFile = null;
		String backupCommand = null;
		String connectionUrl = null;
		Connection connection = null;
		Statement statement = null;
		try {
			
			is = new FileInputStream(fileName);
			prop.load(is);
			
			LOGGER.log(Level.INFO,prop.toString());
			//System.out.println(prop);
			
			connectionUrl = "jdbc:sqlserver://"+prop.getProperty("database.host")+":"+prop.getProperty("database.port")+";"
                        + "database="+prop.getProperty("database.name")+";"
                        + "user="+prop.getProperty("database.user")+";"
                        + "password="+prop.getProperty("database.pass")+";"
                        + "loginTimeout=30;";
			LOGGER.log(Level.INFO,connectionUrl);
						
			connection = DriverManager.getConnection(connectionUrl);
			statement = connection.createStatement();
			
			try{
				Path path = Paths.get(prop.getProperty("backup.full.folder"));
				Files.createDirectories(path);
			}catch(Exception e){}
			
			backupFile = new File(prop.getProperty("backup.full.folder")+File.separator+prop.getProperty("database.name")+"_"+System.currentTimeMillis()+".bak");
			
			backupCommand = "BACKUP DATABASE ["+prop.getProperty("database.name")+"] TO  DISK = N'"+backupFile.getAbsolutePath()+"' WITH NOFORMAT, INIT, NOSKIP, REWIND, NOUNLOAD,  STATS = 10";
			
			LOGGER.log(Level.INFO,backupCommand);
			
			statement.execute(backupCommand);
			
		} catch (Exception ex) {
			LOGGER.log( Level.SEVERE, ex.toString() );
		} finally{
			connection.close();
		}			
	}
	
}