/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.beans;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *
 * @author shadl
 */
public class AlarmRecordBean implements Serializable {

   
    private int installationID;
    private int fileID;
    private java.util.Date almTime;
    private String system;
    private String subSystem;
    private String tagName;
    private String messageType;
    private String priority;
    private String status;
    private String description;

    public AlarmRecordBean() {

    }

    
    private String limitStringSize(String d, int length) {
        if (d != null && d.length() > length) {
            d = d.substring(length - 1);
        }
        return d;
    }

    public void setFileID(int i) {
        fileID = i;
    }

    public void setInstallationID(int i) {
        installationID = i;
    }

    public void setAlarmTime(java.util.Date d) {
        almTime = d;
    }

    public void setSystem(String s) {
        system = limitStringSize(s, 64);
    }

    public void setSubSystem(String s) {
        subSystem = limitStringSize(s, 64);
    }

    public void setTagName(String s) {
        tagName = limitStringSize(s, 32);
    }

    public void setMessageType(String s) {
        messageType = limitStringSize(s, 64);
    }

    public void setPriority(String s) {
        priority = limitStringSize(s, 16);
    }

    public void setStatus(String s) {
        status = limitStringSize(s, 16);
    }

    public void setDescription(String s) {
        description = limitStringSize(s, 255);
    }

    public int getFileID() {
        return fileID;
    }

    public int getInstallationID() {
        return installationID;
    }

    public java.util.Date getAlarmTime() {
        return almTime;
    }

    public String getSystem() {
        return system;
    }

    public String getSubSystem() {
        return subSystem;
    }

    public String getTagName() {
        return tagName;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getPriority() {
        return priority;
    }

    public String getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

}
