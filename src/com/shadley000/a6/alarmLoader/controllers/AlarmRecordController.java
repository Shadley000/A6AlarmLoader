/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.controllers;

import com.shadley000.a6.alarmLoader.beans.AlarmRecordBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *
 * @author shadl
 */
public class AlarmRecordController {

    public final static String sql_InsertAlarm = "INSERT INTO ALARM_STAGING "
            + "(ID_INSTALLATION, ID_ALARM_FILE, ALARM_TIME, ALARM_DATE, SYSTEM,SUBSYSTEM, MESSAGE_TYPE, TAG_NAME, ALARM_PRIORITY, ALARM_STATUS, DESCRIPTION ) "
            + "VALUES (?,?,?,?,?, ?,?,?,?,?); ";

    public final static String sql_removeOldRawDataFromDB = " DELETE from ALARM_STAGING where ID_ALARM_FILE = ?;";
    public final static String sql_removeOldAlarmDataFromDB = " DELETE from ALARM_DATA where ID_ALARM_FILE = ?;";

    PreparedStatement stmt_InsertAlarm = null;
    PreparedStatement stmt_removeOldRawDataFromDB = null;
    PreparedStatement stmt_removeOldAlarmDataFromDB = null;

    public AlarmRecordController(Connection connection) throws SQLException {
        stmt_InsertAlarm = connection.prepareStatement(sql_InsertAlarm);
        stmt_removeOldRawDataFromDB = connection.prepareStatement(sql_removeOldRawDataFromDB);
        stmt_removeOldAlarmDataFromDB = connection.prepareStatement(sql_removeOldAlarmDataFromDB);
    }

    public void deleteAlarmRecords(int fileid) throws SQLException {
        stmt_removeOldRawDataFromDB.setInt(1, fileid);
        stmt_removeOldRawDataFromDB.execute();

        stmt_removeOldAlarmDataFromDB.setInt(1, fileid);
        stmt_removeOldAlarmDataFromDB.execute();
    }

    public void createAlarmRecord(AlarmRecordBean alarmRecord) throws SQLException {
        stmt_InsertAlarm.setInt(1, alarmRecord.getInstallationID());
        stmt_InsertAlarm.setInt(2, alarmRecord.getFileID());
        stmt_InsertAlarm.setTimestamp(3, new Timestamp(alarmRecord.getAlarmTime().getTime()));
        stmt_InsertAlarm.setDate(4, new java.sql.Date(alarmRecord.getAlarmTime().getTime()));
        stmt_InsertAlarm.setString(5, alarmRecord.getSystem());
        stmt_InsertAlarm.setString(6, alarmRecord.getSubSystem());
        stmt_InsertAlarm.setString(7, alarmRecord.getMessageType());
        stmt_InsertAlarm.setString(8, alarmRecord.getTagName());
        stmt_InsertAlarm.setString(9, alarmRecord.getPriority());
        stmt_InsertAlarm.setString(10, alarmRecord.getStatus());
        stmt_InsertAlarm.setString(11, alarmRecord.getDescription());
        stmt_InsertAlarm.execute();
    }

}
