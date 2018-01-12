/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.shadley000.a6.alarmLoader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 *
 * @author shadl
 */
public class CSVLine
{

    private static final String QUOTE = "\"";
    private static final String COMMA = ",";
    private static final String DELIMITERS = QUOTE + COMMA;
    private static final String EMPTY = "";

    private String dataElements[] = null;

    public String getColumn(int index)
    {
        if(index>=dataElements.length) return null;
        return dataElements[index];
    }
    public int getLength(){return dataElements.length;}
    
    public CSVLine(String line)
    {
        List<String> columnDataList = new ArrayList<>();

        StringTokenizer tok = new StringTokenizer(line, DELIMITERS, true);

        int column = 0;
        String columnData = EMPTY;
        boolean inquotes = false;
        while (tok.hasMoreTokens())
        {
            String data = tok.nextToken();

            if (data.equals(QUOTE))
            {
                if (inquotes)
                {
                    inquotes = false;
                } else
                {
                    if (columnData.equals(EMPTY))
                    {
                        inquotes = true;
                    } else
                    {
                        columnData += QUOTE;
                    }
                }
            } else if (data.equals(COMMA))
            {
                if (!inquotes)
                {
                    columnDataList.add(columnData);
                    column++;
                    columnData = EMPTY;
                } else
                {
                    columnData += COMMA;
                }
            } else
            {
                if (!inquotes)
                {
                    columnData += data.trim();
                } else
                {
                    columnData += data;
                }
            }
        }
        columnDataList.add(columnData);

        dataElements = new String[columnDataList.size()];

        column = 0;
        for (Iterator<String> it = columnDataList.iterator(); it.hasNext();)
        {
            dataElements[column] = it.next();
            column++;
        }
    }

    public void display()
    {
        for (int i = 0; i < dataElements.length; i++)
        {
            System.out.println(dataElements[i]);
        }
        System.out.println();
        System.out.println("****************************");
    }
    
    public String toString()
    {   StringBuffer sb = new StringBuffer();
        for (int i = 0; i < dataElements.length; i++)
        {
            sb.append(dataElements[i]);
            sb.append(", ");
        }
        return sb.toString();
    }
    
    public static void main(String args[])
    {

        String strs[] =
        {
            "data", "\"data\"", "", "\"\"",
            "\"data", "da\"ta", "data\"",
            "\"da\"ta", "\"\"data",
            " \"data\"", " \"data\" "
        };

        String data[] =
        {
            "data1,data2,data3",
            "data1, data2, data3",
            "data1, data2, data3",
            " data1, data2, data3",
            "data1,dat\"a2,data3",
            "\"data1\",\"data2\",\"data3\"",
            " \"data1\", \"data2\", \"data3\"",
            "\"data1\",\"data2,more\",\"data3\"",
            "\"data1\",\"data2, more\",\"data3\"",
            "data1,\"data2\",data3",
            "data1,\"data2\",\"data3",
            "data1,\"da\"ta2\",\"data3",
            "data1,\"da\"ta2\"more,\"data3",
            "data1,\"da\"ta2\"more\",\"data3"
        };

        for (int i = 0; i < data.length; i++)
        {
            System.out.println(data[i]);
            new CSVLine(data[i]).display();
        }
    }
}
