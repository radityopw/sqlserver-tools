package sqlserver_tools;

import java.util.logging.*;
import java.util.*;
import java.io.*;
import java.sql.*;
import java.nio.file.*;
import org.apache.commons.io.*;
import java.net.*;

public class RestoreLog{
	
	private static final Logger LOGGER = Logger.getLogger( RestoreLog.class.getName() );
	
	public static void main(String[] a) throws Exception{
		
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new SimpleFormatter());
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
				
		Properties prop = new Properties();
		String fileName = "app.config";
        String sql = null;
		InputStream is = null;
		String connectionUrlSource = null;
		Connection connectionSource = null;
		Statement statementSource = null;
        String connectionUrlTarget = null;
		Connection connectionTarget = null;
		Statement statementTarget = null;
        String connectionUrlWitness = null;
		Connection connectionWitness = null;
		PreparedStatement stmtCekDataExistWitness = null;
		PreparedStatement stmtInsertDataWitness = null;
		PreparedStatement stmtCekAntrianWitness = null;
		PreparedStatement stmtUpdtAntrianWitness = null;
		try {
			
			is = new FileInputStream(fileName);
			prop.load(is);
			
			LOGGER.log(Level.INFO,prop.toString());
			//System.out.println(prop);
			
			connectionUrlSource = "jdbc:sqlserver://"+prop.getProperty("restore.db.source.host")+":"+prop.getProperty("restore.db.source.port")+";"
                        + "database="+prop.getProperty("restore.db.source.name")+";"
                        + "user="+prop.getProperty("restore.db.source.user")+";"
                        + "password="+prop.getProperty("restore.db.source.pass")+";"
                        + "loginTimeout=30;";
			LOGGER.log(Level.INFO,connectionUrlSource);
						
			connectionSource = DriverManager.getConnection(connectionUrlSource);
			statementSource = connectionSource.createStatement();
            
            
            // untuk koneksi ke target kita akan menggunakan master database
            connectionUrlTarget = "jdbc:sqlserver://"+prop.getProperty("restore.db.target.host")+":"+prop.getProperty("restore.db.target.port")+";"
                        + "database=MASTER;"
                        + "user="+prop.getProperty("restore.db.target.user")+";"
                        + "password="+prop.getProperty("restore.db.target.pass")+";"
                        + "loginTimeout=30;";
			LOGGER.log(Level.INFO,connectionUrlTarget);
						
			connectionTarget = DriverManager.getConnection(connectionUrlTarget);
			statementTarget = connectionTarget.createStatement();
            
            connectionUrlWitness = "jdbc:sqlserver://"+prop.getProperty("restore.db.witness.host")+":"+prop.getProperty("restore.db.witness.port")+";"
                        + "database="+prop.getProperty("restore.db.witness.name")+";"
                        + "user="+prop.getProperty("restore.db.witness.user")+";"
                        + "password="+prop.getProperty("restore.db.witness.pass")+";"
                        + "loginTimeout=30;";
			LOGGER.log(Level.INFO,connectionUrlWitness);
						
			connectionWitness = DriverManager.getConnection(connectionUrlWitness);
            
            sql = "SELECT count(*) AS jml FROM log_backup_restore WHERE db_source=? AND db_target=? AND backup_start_date=?";
            
            stmtCekDataExistWitness = connectionWitness.prepareStatement(sql);
            
            sql = "INSERT INTO log_backup_restore(db_source,db_target,backup_start_date,first_lsn,last_lsn,status,backup_file) VALUES(?,?,?,?,?,?,?)";
            
            stmtInsertDataWitness = connectionWitness.prepareStatement(sql);
            
            sql = "SELECT * FROM log_backup_restore WHERE (status=? OR status=?) AND db_target=? ORDER BY backup_start_date";
            
            stmtCekAntrianWitness = connectionWitness.prepareStatement(sql);
			
            sql = "UPDATE log_backup_restore SET status = ?,error_message=? WHERE id = ?";
            
            stmtUpdtAntrianWitness = connectionWitness.prepareStatement(sql);
            
			
			try{
				Path path = Paths.get(prop.getProperty("restore.db.target.temp_folder"));
				Files.createDirectories(path);
			}catch(Exception e){}
            
            /*
            
            ambil data backup 2 hari terakhir
            
            */
            
            
            sql = "SELECT s.database_name, replace(m.physical_device_name,'"+prop.getProperty("restore.db.source.log_dir")+"','') as physical_device_name, CAST(CAST(s.backup_size / 1000000 AS INT) AS VARCHAR(14)) + ' ' + 'MB' AS bkSize, CAST(DATEDIFF(second, s.backup_start_date, s.backup_finish_date) AS VARCHAR(4)) + ' ' + 'Seconds' TimeTaken, s.backup_start_date, CAST(s.first_lsn AS VARCHAR(50)) AS first_lsn, CAST(s.last_lsn AS VARCHAR(50)) AS last_lsn, CASE s.[type] WHEN 'D' THEN 'Full' WHEN 'I' THEN 'Differential' WHEN 'L' THEN 'Transaction Log' END AS BackupType, s.server_name, s.recovery_model FROM msdb.dbo.backupset s INNER JOIN msdb.dbo.backupmediafamily m ON s.media_set_id = m.media_set_id WHERE s.database_name = '"+prop.getProperty("restore.db.source.name")+"' AND s.[type] = 'L' AND s.backup_start_date >= dateadd(d,-2,cast(getDate() as date)) ORDER BY backup_start_date";
            
            LOGGER.log(Level.INFO,sql);
            
            ResultSet rsSource = statementSource.executeQuery(sql);
            
            while(rsSource.next()){
                stmtCekDataExistWitness.setString(1,prop.getProperty("restore.db.source.name"));
                stmtCekDataExistWitness.setString(2,prop.getProperty("restore.db.target.name"));
                stmtCekDataExistWitness.setTimestamp(3,rsSource.getTimestamp("backup_start_date"));
                ResultSet rsWitness = stmtCekDataExistWitness.executeQuery();
                if(rsWitness.next()){
                    if(rsWitness.getInt("jml") == 0){

                        LOGGER.log(Level.INFO,"INSERT DATA LOG BACKUP");

                        stmtInsertDataWitness.setString(1,prop.getProperty("restore.db.source.name"));
                        stmtInsertDataWitness.setString(2,prop.getProperty("restore.db.target.name"));
                        stmtInsertDataWitness.setTimestamp(3,rsSource.getTimestamp("backup_start_date"));
                        stmtInsertDataWitness.setString(4,rsSource.getString("first_lsn"));
                        stmtInsertDataWitness.setString(5,rsSource.getString("last_lsn"));
                        stmtInsertDataWitness.setString(6,"ANTRI");
                        stmtInsertDataWitness.setString(7,rsSource.getString("physical_device_name"));

                        stmtInsertDataWitness.execute();
                    } 
                }
            }
            
            /*
            
            memproses yang masih antri atau error
            
            */
            
            
            stmtCekAntrianWitness.setString(1,"antri");
            stmtCekAntrianWitness.setString(2,"error");
            stmtCekAntrianWitness.setString(3,prop.getProperty("restore.db.target.name"));
            
            ResultSet rsWitnessCekAntri = stmtCekAntrianWitness.executeQuery();
            
            while(rsWitnessCekAntri.next()){
                
                String backupFileSource = prop.getProperty("restore.db.source.file_url")+rsWitnessCekAntri.getString("backup_file");
                
                String backupFileTarget = prop.getProperty("restore.db.target.temp_folder")+File.separator+rsWitnessCekAntri.getString("backup_file");
                
                String id = rsWitnessCekAntri.getString("id");
                
                LOGGER.log(Level.INFO,"processing "+id);
                
                String status = "error";
                String errorMessage = null;
                
                try{
                    //copy log backup
                    FileUtils.copyURLToFile(new URL(backupFileSource), new File(backupFileTarget));
                    
                    sql = "RESTORE LOG ["+prop.getProperty("restore.db.target.name")+"] FROM  DISK = N'"+backupFileTarget+"' WITH  NORECOVERY,  NOUNLOAD,  STATS = 10";
                    
                    LOGGER.log(Level.INFO,sql);
                    
                    statementTarget.execute(sql);
                    
                    status = "OK";
                    
                    stmtUpdtAntrianWitness.setString(1,status);
                    stmtUpdtAntrianWitness.setString(2,errorMessage);
                    stmtUpdtAntrianWitness.setString(3,id);
                    
                    stmtUpdtAntrianWitness.execute();
                    
                    
                    
                    
                }catch(Exception ex){
                    
                    errorMessage = ex.toString();
                    
                    stmtUpdtAntrianWitness.setString(1,status);
                    stmtUpdtAntrianWitness.setString(2,errorMessage);
                    stmtUpdtAntrianWitness.setString(3,id);
                    
                    stmtUpdtAntrianWitness.execute();
                    
                }
            }
            
            
			
		} catch (Exception ex) {
			LOGGER.log( Level.SEVERE, ex.toString() );
            ex.printStackTrace();
		} finally{
            connectionSource.close();
            connectionTarget.close();
			connectionWitness.close();
		}			
	}
	
}