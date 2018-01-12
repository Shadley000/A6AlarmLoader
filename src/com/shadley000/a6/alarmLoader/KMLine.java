package com.shadley000.a6.alarmLoader;

import com.shadley000.a6.alarmLoader.beans.AlarmRecordBean;
import com.shadley000.a6.alarmLoader.exceptions.HeaderLineException;
import com.shadley000.a6.alarmLoader.exceptions.InfoException;
import com.shadley000.a6.alarmLoader.exceptions.NoTagException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class KMLine
{

    public final static String i_alm_event_type = "Event Type";
    public final static String i_alm_time_local = "Time Local";

    public final static String i_alm_time = "Time";
    public final static String i_alm_tagName = "Tag";
    public final static String i_alm_terminal = "Terminal";
    public final static String i_alm_description = "Description";
    public final static String i_alm_station = "Station";
    public final static String i_alm_priority = "Priority";
    public final static String i_alm_eventText = "Event Text";
    public final static String i_alm_state = "Alarm State";
    public final static String i_alm_type = "Type";
    public final static String i_alm_system = "System";
    public final static String i_alm_subsystem = "Sub System";

    public final static SimpleDateFormat timeFormatArr[] =
    {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSSS"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm:ss aa"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm"),
        new SimpleDateFormat("MM/dd/yyyy"),
        new SimpleDateFormat("yyyy-MM-dd")

    };

    final static String validPriorities[] =
    {
        "Low", "High", "Medium", "Critical"
    };
    final static String validAlarmState[] =
    {
        "High", "Low", "LowLow", "HighHigh", "LowScale", "DigAlActive", "Normal", "FALSE"
    };

    public Date parseDate(String data) throws ParseException
    {
        if (data == null || data.trim().length() == 0)
        {
            throw new ParseException("Unable to parse null date", 0);
        }

        for (int i = 0; i < timeFormatArr.length; i++)
        {
            try
            {
                return timeFormatArr[i].parse(data);

            } catch (ParseException e)
            {

            }
        }
        throw new ParseException("Unable to parse date:" + data, 0);
    }

    public boolean validateValues(String data, String[] validValues)
    {
        for (String validValue : validValues)
        {
            if (validValue.equalsIgnoreCase(data))
            {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Integer> findHeaderFormat(CSVLine csvLine) throws ParseException
    {

        Map<String, Integer> columnMap = new HashMap<>();

        for (int i = 0; i < csvLine.getLength(); i++)
        {
            String columnName = csvLine.getColumn(i);
            Integer index = i;

            if (columnName.equalsIgnoreCase(i_alm_time_local))
            {
                columnMap.put(i_alm_time, index);
            } else if (columnName.equalsIgnoreCase(i_alm_event_type))
            {
                columnMap.put(i_alm_type, index);
            } else
            {
                columnMap.put(columnName, index);
            }
        }

        if (columnMap.get(i_alm_time) == null)
        {
            columnMap.put(i_alm_time, 0);
        }
        return columnMap;
    }

    private String getValue(Map<String, Integer> columnMap, CSVLine csvLine, String tag)
    {
        Integer column = columnMap.get(tag);
        if (column == null || column >= csvLine.getLength())
        {
            return null;
        }
        String data = csvLine.getColumn(column);
        return data;
    }

    public AlarmRecordBean parse(Map<String, Integer> columnMap, CSVLine csvLine) throws ParseException, HeaderLineException, InfoException, NoTagException
    {

        if (csvLine.getColumn(0).startsWith("Time"))
        {
            throw new HeaderLineException();
        }

        AlarmRecordBean alarmRecord = new AlarmRecordBean();

        alarmRecord.setAlarmTime(parseDate(getValue(columnMap, csvLine, i_alm_time)));
        alarmRecord.setSystem(getValue(columnMap, csvLine, i_alm_system));
        alarmRecord.setSubSystem(getValue(columnMap, csvLine, i_alm_subsystem));
        alarmRecord.setTagName(getValue(columnMap, csvLine, i_alm_tagName));
        alarmRecord.setMessageType(getValue(columnMap, csvLine, i_alm_eventText));
        alarmRecord.setPriority(getValue(columnMap, csvLine, i_alm_priority));
        alarmRecord.setStatus(getValue(columnMap, csvLine, i_alm_state));
        alarmRecord.setDescription(getValue(columnMap, csvLine, i_alm_description));

        if (alarmRecord.getTagName() != null && alarmRecord.getTagName().length() == 0)
        {
            throw new NoTagException(csvLine.toString());
        }
        if (alarmRecord.getPriority() != null && alarmRecord.getPriority().equalsIgnoreCase("info"))
        {
            throw new InfoException("info message");
        }

        if (alarmRecord.getPriority() == null)
        {
            throw new InfoException("No Priority:" + alarmRecord.getStatus());
        }

        if (!validateValues(alarmRecord.getPriority(), validPriorities))
        {
            throw new ParseException("Unrecognized priority value:" + alarmRecord.getPriority(), 0);
        }

        if (!validateValues(alarmRecord.getStatus(), validAlarmState))
        {
            throw new ParseException("Unrecognized alm_state value:" + alarmRecord.getStatus(), 0);
        }

        String alm_type = getValue(columnMap, csvLine, i_alm_type);

        if (alm_type != null)
        {
            if (alm_type.contains("Message"))
            {
                throw new InfoException("message alarm");
            }
        }
        return alarmRecord;
    }

}
