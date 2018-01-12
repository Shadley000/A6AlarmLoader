package com.shadley000.a6.alarmLoader;

import com.shadley000.a6.alarmLoader.controllers.AlarmFileController;
import com.shadley000.a6.alarmLoader.controllers.ExceptionController;
import com.shadley000.a6.alarmLoader.controllers.AlarmRecordController;
import com.shadley000.a6.alarmLoader.beans.AlarmRecordBean;
import com.shadley000.a6.alarmLoader.beans.InstallationBean;
import com.shadley000.a6.alarmLoader.controllers.InstallationController;
import com.shadley000.a6.alarmLoader.exceptions.FileFormatException;
import com.shadley000.a6.alarmLoader.exceptions.HeaderLineException;
import com.shadley000.a6.alarmLoader.exceptions.InfoException;
import com.shadley000.a6.alarmLoader.exceptions.LoadFileException;
import com.shadley000.a6.alarmLoader.exceptions.NoTagException;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.PrintStream;
import static java.lang.System.getProperties;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadToDatabase {

    AlarmFileController alarmFileController;
    ExceptionController exceptionController;
    AlarmRecordController alarmRecordController;
    InstallationController installationController;

    PreparedStatement stmt_getVendorID = null;

   // static PrintStream skippedOut = null;

    public ReadToDatabase(Connection connection) throws Exception {

        alarmFileController = new AlarmFileController(connection);
        exceptionController = new ExceptionController(connection);
        alarmRecordController = new AlarmRecordController(connection);
        installationController = new InstallationController(connection);
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("USAGE " + ReadToDatabase.class.getName() + " directory database.properties");
            System.out.println("\tLoads alarm files from the directory's subdirectories");
            return;
        }
        String directory = args[0];
        String propertiesFileName = args[1];

        Connection connection;

        Properties properties = new Properties();
        try {
            
            properties.load(new FileReader(propertiesFileName));
            Class.forName(properties.getProperty("driver"));
            System.out.println("Connecting to database");
            System.out.println("\turl:"+properties.getProperty("url"));
            System.out.println("\tuser:"+properties.getProperty("user"));
            System.out.println("\tpassword"+properties.getProperty("password"));
            connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("user"),properties.getProperty("password"));
        } catch (Exception ex) {
            Logger.getLogger(ReadToDatabase.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        System.out.println("Loading alarm files from directory " + directory);
        File InstallationsDirectory = new File(directory);

        ReadToDatabase readToDatabase = null;
        try {

            readToDatabase = new ReadToDatabase(connection);

            if (InstallationsDirectory.isDirectory()) {
                String[] installationFileList = InstallationsDirectory.list();

                for (int i = 0; i < installationFileList.length; i++) {
                    File installationFile = new File(installationFileList[i]);
                    if (installationFile.isDirectory()) {
                        InstallationBean installationBean = readToDatabase.installationController.find(installationFile.getName());
                        if (installationBean != null) {
                            readToDatabase.loadInstallationDirectory(installationBean, installationFile);
                        }
                    }
                }
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public void loadInstallationDirectory(InstallationBean installationBean, File installationFile) {
        try {
            System.out.println("Loading Installation " + installationBean.getName() + " " + installationBean.getId());

            String[] importFileList = installationFile.list();
            for (int f = 0; f < importFileList.length; f++) {
                String fileName = importFileList[f];
                File importFile = new File(installationFile.getAbsoluteFile() + "/" + fileName);

                if (importFile.isFile() && (fileName.endsWith(".csv") || fileName.endsWith(".CSV"))) {
                    System.out.println("importing  " + importFile.getAbsolutePath());

                    try {
                        importToDatabase(installationBean, importFile.getAbsolutePath());

                        Path source = FileSystems.getDefault().getPath(importFile.getParent(), fileName);
                        Path target = FileSystems.getDefault().getPath(importFile.getParent() + "/archive/", fileName);

                        //System.out.println("move " + source.toString() + " " + target.toString());
                        Files.move(source, target, REPLACE_EXISTING);
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public void importToDatabase(InstallationBean installationBean, String fileName) throws FileFormatException, FileNotFoundException, IOException, SQLException, LoadFileException {

        removeOldFileFromDB(installationBean.getId(), fileName);

        File csvFile = new File(fileName);

        int lineNum = 0;
        int outputCount = 0;
        int exceptionCount = 0;
        int skippedCount = 0;

        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        String nextLine;

        Map<String, Integer> columnMap = null;

        int fileID = -1;
        String parserName = installationBean.getParserName();
        int installationID = installationBean.getId();

        while ((nextLine = reader.readLine()) != null) {
            CSVLine csvLine = new CSVLine(nextLine);
            if (columnMap == null) {
                try {
                    if (parserName.equals("NOV1")) {
                        columnMap = NOVLine.findHeaderFormat(csvLine);
                    } else if (parserName.equals("KM1")) {
                        columnMap = KMLine.findHeaderFormat(csvLine);
                    }
                    fileID = alarmFileController.createNewFileInDB(installationBean.getId(), csvFile.getName());
     
                } catch (ParseException e) {
                    throw new FileFormatException("Fatal file format exception:" + e.getClass().getName() + " " + e.getMessage());
                }
            } else {
                AlarmRecordBean alarm = null;

                try {
                    if (parserName.equals("NOV1")) {
                        NOVLine novLine = new NOVLine();
                        alarm = novLine.parse(columnMap, csvLine);
                    } else if (parserName.equals("KM1")) {
                        KMLine kmLine = new KMLine();
                        alarm = kmLine.parse(columnMap, csvLine);
                    }

                    if (alarm != null) {
                        alarm.setInstallationID(installationID);
                        alarm.setFileID(fileID);
                        alarmRecordController.createAlarmRecord(alarm);
                        outputCount++;
                    }

                } catch (HeaderLineException e) {
                    //ignore
                } catch (InfoException | NoTagException e) {
                    skippedCount++;
                } catch (ParseException e) {
                    exceptionCount++;
                    exceptionController.createException(installationID, fileID, e.getClass().getName() + " " + e.getMessage(), "", lineNum);
                } catch (SQLException e) {	//System.out.println("Description:"+alarm.getDescription());
                    throw e;
                }
            }

            lineNum++;
        }
        reader.close();

        alarmFileController.updateFileData(installationID, fileID, lineNum, outputCount, skippedCount, exceptionCount);

        System.out.println(outputCount + " records inserted, " + skippedCount + " skipped " + exceptionCount + " errors");
    }

    public void removeOldFileFromDB(int installationID, String filename) throws SQLException {
        Integer fileID = alarmFileController.getFileID(installationID, filename);

        if (fileID != null) {
            alarmRecordController.deleteAlarmRecords(fileID);
            exceptionController.removeExceptions(installationID, fileID);
            alarmFileController.removeOldFileFromDB(installationID, fileID);

        }
    }

}
