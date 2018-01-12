/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader.beans;

import java.io.Serializable;

/**
 *
 * @author shadl
 */
public class InstallationBean  implements Serializable{
    private int id;
    private int idVendor;
    private int idShip;
    private int idContractor;
    private String name;
    private String dataDirectory;

    public String getDataDirectory() {
        return dataDirectory;
    }

    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }
    private String parserName;
    

    public InstallationBean() {
    }

    public InstallationBean(int idVendor, int idShip, int idContractor, String name,String parserName) {
        this.idVendor = idVendor;
        this.idShip = idShip;
        this.idContractor = idContractor;
        this.name = name;
        this.parserName = parserName;
    }

    public String getParserName() {
        return parserName;
    }

    public void setParserName(String parserName) {
        this.parserName = parserName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getIdVendor() {
        return idVendor;
    }

    public void setIdVendor(int idVendor) {
        this.idVendor = idVendor;
    }

    public int getIdShip() {
        return idShip;
    }

    public void setIdShip(int idShip) {
        this.idShip = idShip;
    }

    public int getIdContractor() {
        return idContractor;
    }

    public void setIdContractor(int idContractor) {
        this.idContractor = idContractor;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

            
}
