/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.controllers;

import com.shadley000.a6.alarmLoader.beans.InstallationBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author shadl
 */
public class PivotController {

    Connection connection;

    String sql_CallPivotUpdate = "CALL ALARM_PIVOT_UPDATE(?, ?)";

    String sql_FindCalculatedIDs = "Select ID, TAG_NAME from ALARM_TYPE where ID_INSTALLATION = ? and MESSAGE_TYPE = 'CALCULATED'";

    String sql_SelectACSRows = "SELECT t.SUBSYSTEM, d.ALARM_TIME "
            + " FROM ALARM_DATA d, ALARM_TYPE t"
            + " where "
            + " t.ID = d.ID_ALARM_TYPE "
            + " AND t.ID_INSTALLATION = ? "
            + " AND d.ALARM_DATE = ? "
            + " AND t.SYSTEM = ? "
            + " AND d.ALARM_STATUS = 'CFN' "
            + " AND t.DESCRIPTION LIKE '%topped%' ";

    String SQL_BASE = "SELECT count(*) "
            + " FROM ALARM_DATA d, ALARM_TYPE t"
            + " where "
            + " t.ID = d.ID_ALARM_TYPE "
            + " AND t.ID_INSTALLATION = ? "
            + " AND d.ALARM_DATE = ? "
            + " AND d.ALARM_STATUS = 'CFN' ";

    String sql_SelectHR1Rows = SQL_BASE + " AND t.SUBSYSTEM = ? AND (t.DESCRIPTION LIKE '%Control Error%' OR t.DESCRIPTION LIKE '%Control Fault%')  ";
    String sql_SelectSDIRows = SQL_BASE + " AND t.SUBSYSTEM = ? AND (t.DESCRIPTION LIKE '%eviation%')";
    String sql_SelectSDIDeviationRows = SQL_BASE + " AND t.SUBSYSTEM = ?";
    String sql_SelectDWRows = SQL_BASE + " AND t.SUBSYSTEM = ? AND (t.DESCRIPTION LIKE '%Brake%')";

    String sql_CleanPivotData = "DELETE FROM ALARM_PIVOT where ID_INSTALLATION = ? AND ALARM_DATE = ?";
    String sql_InsertPivotRow = "INSERT INTO ALARM_PIVOT (ID_INSTALLATION,ID_ALARM_TYPE,ALARM_COUNT,ALARM_DATE) VALUES (?,?,?,?)";

    PreparedStatement stmt_CallPivotUpdate;
    PreparedStatement stmt_FindCalculatedIDs;

    PreparedStatement stmt_SelectACSRows;
    PreparedStatement stmt_SelectHR1Rows;
    PreparedStatement stmt_SelectSDIRows;
    PreparedStatement stmt_SelectSDIDeviationRows;
    PreparedStatement stmt_SelectDWRows;
    PreparedStatement stmt_InsertPivotRow;
    PreparedStatement stmt_CleanPivotData;

    Map<String, Integer> calculatedRowIDs;

    public PivotController(Connection connection) throws SQLException {
        this.connection = connection;
        stmt_CallPivotUpdate = connection.prepareStatement(sql_CallPivotUpdate);
        stmt_FindCalculatedIDs = connection.prepareStatement(sql_FindCalculatedIDs);

        stmt_SelectACSRows = connection.prepareStatement(sql_SelectACSRows);
        stmt_SelectHR1Rows = connection.prepareStatement(sql_SelectHR1Rows);
        stmt_SelectSDIRows = connection.prepareStatement(sql_SelectSDIRows);
        stmt_SelectSDIDeviationRows = connection.prepareStatement(sql_SelectSDIDeviationRows);
        stmt_SelectDWRows = connection.prepareStatement(sql_SelectDWRows);
        stmt_InsertPivotRow = connection.prepareStatement(sql_InsertPivotRow);
        stmt_CleanPivotData = connection.prepareStatement(sql_CleanPivotData);
    }

    public void updatePivots(InstallationBean installationBean, Set<String> dateSet) throws SQLException {
        for (String aDate : dateSet) {
            
            stmt_CleanPivotData.setInt(1, installationBean.getId());
            stmt_CleanPivotData.setString(2, aDate);
            stmt_CleanPivotData.execute();
                    
            stmt_CallPivotUpdate.setString(1, aDate);
            stmt_CallPivotUpdate.setInt(2, installationBean.getId());
            stmt_CallPivotUpdate.execute();
        }
    }

    public void loadCalculatedRowIDs(InstallationBean installationBean) throws SQLException {
        calculatedRowIDs = new HashMap<>();
        stmt_FindCalculatedIDs.setInt(1, installationBean.getId());
        ResultSet rs = stmt_FindCalculatedIDs.executeQuery();

        while (rs.next()) {
            Integer id = rs.getInt("ID");
            String tagName = rs.getString("TAG_NAME");
            calculatedRowIDs.put(tagName, id);
        }
    }

    public void generateCalculatedRows(InstallationBean installationBean, Set<String> dateSet) throws SQLException {
        //not only is this an ugly hardcoded mess, but it is slow too......
        loadCalculatedRowIDs(installationBean);
        SimpleDateFormat df = new SimpleDateFormat("a");
        int installationID = installationBean.getId();

        for (String aDate : dateSet) {
            int ACS_MAIN_STOP_count = 0;
            int ACS_MAIN_AM_STOP_count = 0;
            int ACS_MAIN_PM_STOP_count = 0;
            int ACS_AUX_STOP_count = 0;
            int ACS_AUX_AM_STOP_count = 0;
            int ACS_AUX_PM_STOP_count = 0;
            stmt_SelectACSRows.setInt(1, installationID);
            stmt_SelectACSRows.setString(2, aDate);
            stmt_SelectACSRows.setString(3, "ACS");
            ResultSet rs = stmt_SelectACSRows.executeQuery();
            while (rs.next()) {
                String subSystem = rs.getString(1);
                Date alarmTime = rs.getTimestamp(2);
                boolean isAM = "AM".equalsIgnoreCase(df.format(alarmTime));
                if (subSystem.equalsIgnoreCase("ACS")) {
                    ACS_MAIN_STOP_count++;
                    if (isAM) {
                        ACS_MAIN_AM_STOP_count++;
                    } else {
                        ACS_MAIN_PM_STOP_count++;
                    }
                } else {
                    ACS_AUX_STOP_count++;
                }
                if (isAM) {
                    ACS_AUX_AM_STOP_count++;
                } else {
                    ACS_AUX_PM_STOP_count++;
                }
            }

            insertPivotRow(installationID, aDate, "ACS_MAIN_STOP", ACS_MAIN_STOP_count);
            insertPivotRow(installationID, aDate, "ACS_MAIN_AM_STOP", ACS_MAIN_AM_STOP_count);
            insertPivotRow(installationID, aDate, "ACS_MAIN_PM_STOP", ACS_MAIN_PM_STOP_count);
            insertPivotRow(installationID, aDate, "ACS_AUX_STOP", ACS_AUX_STOP_count);
            insertPivotRow(installationID, aDate, "ACS_AUX_AM_STOP", ACS_AUX_AM_STOP_count);
            insertPivotRow(installationID, aDate, "ACS_AUX_PM_STOP", ACS_AUX_PM_STOP_count);

            insertPivotRow(installationID, aDate, "SDI_TOTAL", getCount(stmt_SelectSDIRows, installationID, aDate, "SDI"));
            insertPivotRow(installationID, aDate, "SDI2_TOTAL", getCount(stmt_SelectSDIRows, installationID, aDate, "SDI2"));
            insertPivotRow(installationID, aDate, "SDI_DEVIATION", getCount(stmt_SelectSDIDeviationRows, installationID, aDate, "SDI"));
            insertPivotRow(installationID, aDate, "SDI2_DEVIATION", getCount(stmt_SelectSDIDeviationRows, installationID, aDate, "SDI2"));
            insertPivotRow(installationID, aDate, "HR1_CALIBRATION", getCount(stmt_SelectHR1Rows, installationID, aDate, "HR1"));
            insertPivotRow(installationID, aDate, "HR2_CALIBRATION", getCount(stmt_SelectHR1Rows, installationID, aDate, "HR2"));
            insertPivotRow(installationID, aDate, "DW1_BRAKE", getCount(stmt_SelectDWRows, installationID, aDate, "DW1"));
            insertPivotRow(installationID, aDate, "DW2_BRAKE", getCount(stmt_SelectDWRows, installationID, aDate, "DW2"));

        }

    }

    protected void insertPivotRow(int installationID, String aDate, String tagName, int count) throws SQLException {
        if (count == 0) {
            return;
        }
        if(calculatedRowIDs.get(tagName)==null)
        {
            System.out.println("Custom Calculated tage missing:"+tagName);
        }
        else{
        stmt_InsertPivotRow.setInt(1, installationID);
        stmt_InsertPivotRow.setInt(2, calculatedRowIDs.get(tagName));
        stmt_InsertPivotRow.setInt(3, count);
        stmt_InsertPivotRow.setString(4, aDate);
        stmt_InsertPivotRow.execute();}
    }

    protected int getCount(PreparedStatement stmt, int installationID, String aDate, String subSystem) throws SQLException {
        stmt.setInt(1, installationID);
        stmt.setString(2, aDate);
        stmt.setString(3, subSystem);
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {

            return rs.getInt(1);
        }
        return 0;
    }
}
