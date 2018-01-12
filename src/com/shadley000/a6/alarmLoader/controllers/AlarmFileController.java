/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.controllers;

import com.shadley000.a6.alarmLoader.exceptions.LoadFileException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author shadl
 */
public class AlarmFileController {

    public final static String sql_removeOldFileFromDB = " DELETE from ALARMFILE where ID_INSTALLATION = ? AND ID = ?;";
    public final static String sql_createNewFileInDB = "insert into ALARMFILE (ID_INSTALLATION, FILE_NAME) values (?,?);";
    public final static String sql_getFileID = "select ID from ALARMFILE where ID_INSTALLATION = ? AND FILE_NAME = ?;";
    private final static String sql_Update_File = "UPDATE ALARMFILE SET DATA_LINES=?, DATA_INSERTED=?, DATA_SKIPPED=?, DATA_ERROR=? WHERE ID_INSTALLATION = ? AND ID = ?;";

    PreparedStatement stmt_getFileID = null;
    PreparedStatement stmt_removeOldFileFromDB = null;
    PreparedStatement stmt_createNewFileInDB = null;
    PreparedStatement stmt_updateFileData = null;

    public AlarmFileController(Connection connection) throws SQLException {
        stmt_getFileID = connection.prepareStatement(sql_getFileID);
        stmt_updateFileData = connection.prepareStatement(sql_Update_File);
        stmt_createNewFileInDB = connection.prepareStatement(sql_createNewFileInDB);
        stmt_removeOldFileFromDB = connection.prepareStatement(sql_removeOldFileFromDB);
    }

    public Integer getFileID(int installationID, String filename) throws SQLException {
        stmt_getFileID.setInt(1, installationID);
        stmt_getFileID.setString(2, filename);
        ResultSet rs = stmt_getFileID.executeQuery();

        if (rs.next()) {
            int fileID = rs.getInt(1);
        }
        return null;
    }

    public void removeOldFileFromDB(int installationID, int fileID) throws SQLException {

        stmt_removeOldFileFromDB.setInt(1, installationID);
        stmt_removeOldFileFromDB.setInt(2, fileID);
        stmt_removeOldFileFromDB.execute();

    }

    public int createNewFileInDB(int installationID, String filename) throws SQLException, LoadFileException {
        stmt_createNewFileInDB.setInt(1, installationID);
        stmt_createNewFileInDB.setString(2, filename);
        stmt_createNewFileInDB.execute();

        stmt_getFileID.setInt(1, installationID);
        stmt_getFileID.setString(2, filename);
        ResultSet rs = stmt_getFileID.executeQuery();

        if (rs.next()) {
            return rs.getInt(1);
        } else {
            throw new LoadFileException("Unable to create fileID for  " + filename);
        }
    }

    public void updateFileData(int installationID, int file_id, int lines, int inserted, int skipped, int errors) throws SQLException {
        stmt_updateFileData.setInt(1, lines);
        stmt_updateFileData.setInt(2, inserted);
        stmt_updateFileData.setInt(3, skipped);
        stmt_updateFileData.setInt(4, errors);

        stmt_updateFileData.setInt(5, installationID);
        stmt_updateFileData.setInt(6, file_id);

        stmt_updateFileData.execute();
    }
}
