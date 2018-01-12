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

/**
 *
 * @author shadl
 */
public class InstallationController {

    private final static String sql_selectInstallationID = "SELECT ID,ID_VENDOR,ID_SHIP,ID_CONTRACTOR,NNAME,DATADIRECTORY,PARSERNAME from INSTALLATION where DATADIRECTORY = ?;";
    PreparedStatement stmt_getInstallationID = null;

    public InstallationController(Connection connection) throws SQLException {
        stmt_getInstallationID = connection.prepareStatement(sql_selectInstallationID);
    }

    public InstallationBean find(String directoryName) throws SQLException {
        stmt_getInstallationID.setString(1, directoryName);
        ResultSet rs = stmt_getInstallationID.executeQuery();

        while (rs.next()) {
            InstallationBean bean = new InstallationBean();
            bean.setId(rs.getInt(1));
            bean.setIdVendor(rs.getInt(2));
            bean.setIdShip(rs.getInt(3));
            bean.setIdContractor(rs.getInt(4));
            bean.setName(rs.getString(5));
            bean.setDataDirectory(rs.getString(6));
            bean.setParserName(rs.getString(7));
            return bean;
        }
        return null;
    }
}
