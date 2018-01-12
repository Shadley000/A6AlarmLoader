/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.controllers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author shadl
 */
public class ExceptionController {

    private final static String sql_removeOldErrorsFromDB = "DELETE from ERRORLOG where ID_INSTALLATION = ? AND ID_FILE = ?;";

    private final static String sql_InsertError = "insert into ERRORLOG (ID_INSTALLATION, ID_FILE, ERROR_MESSAGE, ORIGINAL_TEXT, LINENUMBER) values (?,?,?,?,?);";
    private PreparedStatement stmt_removeOldErrorsFromDB = null;
    private PreparedStatement stmt_logError = null;

    public ExceptionController(Connection connection) throws SQLException {
        stmt_removeOldErrorsFromDB = connection.prepareStatement(sql_removeOldErrorsFromDB);
        stmt_logError = connection.prepareStatement(sql_InsertError);
    }

    public void removeExceptions(int installationID, int fileID) throws SQLException {
        stmt_removeOldErrorsFromDB.setInt(1, installationID);
        stmt_removeOldErrorsFromDB.setInt(2, fileID);
        stmt_removeOldErrorsFromDB.execute();
    }

    public void createException(int installationID, int fileID, String message, String text, int lineNum) throws SQLException {
        if (message != null && message.length() > 512) {
            message = message.substring(0, 511);
        }
        stmt_logError.setInt(1, installationID);
        stmt_logError.setInt(2, fileID);
        stmt_logError.setString(3, message);
        stmt_logError.setString(4, text);
        stmt_logError.setInt(5, lineNum);
        stmt_logError.execute();
    }
}
