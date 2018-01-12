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

public class NOVLine
{

    public final static String i_ALM_ID = "ALM_ID";
    public final static String i_ALM_DATELAST = "ALM_DATELAST";
    public final static String i_ALM_TIMELAST = "ALM_TIMELAST";
    public final static String i_ALM_LOGNODENAME = "ALM_LOGNODENAME";
    public final static String i_ALM_TAGNAME = "ALM_TAGNAME";
    public final static String i_ALM_DESCR = "ALM_DESCR";
    public final static String i_ALM_VALUE = "ALM_VALUE";
    public final static String i_ALM_UNIT = "ALM_UNIT";
    public final static String i_ALM_ALMSTATUS = "ALM_ALMSTATUS";
    public final static String i_ALM_MSGTYPE = "ALM_MSGTYPE";
    public final static String i_ALM_ALMPRIORITY = "ALM_ALMPRIORITY";
    public final static String i_ALM_ALMAREA = "ALM_ALMAREA";
    public final static String i_ALM_TIMEIN = "ALM_TIMEIN";
    public final static String i_ALM_DATEIN = "ALM_DATEIN";
    public final static String i_ALM_OPNAME = "ALM_OPNAME";
    public final static String i_ALM_OPNODE = "ALM_OPNODE";
    public final static String i_ALM_PHYSLNODE = "ALM_PHYSLNODE";
    public final static String i_ALM_ALMEXTFLD1 = "ALM_ALMEXTFLD1";
    public final static String i_ALM_ALMEXTFLD2 = "ALM_ALMEXTFLD2";
    public final static String i_ALM_NATIVETIMEIN = "ALM_NATIVETIMEIN";
    public final static String i_ALM_NATIVETIMELAST = "ALM_NATIVETIMELAST";
    public final static String i_Condition = "Condition";
    public final static String i_SubCondition = "SubCondition";
    public final static String i_ALM_HIDEREASON = "ALM_HIDEREASON";

    public final static SimpleDateFormat dateFormatArr[] =
    {
        new SimpleDateFormat("dd.MM.yyyy"),
        new SimpleDateFormat("MM-dd-yyyy"),
        new SimpleDateFormat("MM/dd/yyyy"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm")
    };

    public final static SimpleDateFormat timeFormatArr[] =
    {
        new SimpleDateFormat("HH:mm:ss.SSS"),
        new SimpleDateFormat("HH:mm:ss.SS"),
        new SimpleDateFormat("HH:mm:ss.S"),
        new SimpleDateFormat("HH:mm.ss"),
        new SimpleDateFormat("HH:mm:s"),
        new SimpleDateFormat("HH:mm")
    };

    public final static String validPriortity[] =
    {
        "HIGH", "LOW", "MEDIUM"
    };
    public final static String validStatus[] =
    {
        "OK", "CFN"
    };

   public final static String validStatusMessage[] =
    {
    };

    //_ALM_LOGNODENAME = {"SERV","TERM"};
    public final static String validMessageType[] =
    {
        "ALARM", "COMM", "OVER", "OPERATOR", "NETWORK", "EVENT", "TEXT", "SYSALERT"
    };//messagetype "ALARM",,blank

    public static Map<String, Integer> findHeaderFormat(CSVLine csvLine) throws ParseException
    {

        Map<String, Integer> columnMap = new HashMap<String, Integer>();

        for (int i = 0; i < csvLine.getLength(); i++)
        {
            String columnName = csvLine.getColumn(i);
            Integer index = new Integer(i);
            columnMap.put(columnName, index);
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

    public boolean validateValues(String data, String[] validValues)
    {
        for (int i = 0; i < validValues.length; i++)
        {
            if (validValues[i].equalsIgnoreCase(data))
            {
                return true;
            }
        }
        return false;
    }

    public Date parseDate(String data) throws ParseException
    {
        if (data == null || data.trim().length() == 0)
        {
            throw new ParseException("Unable to parse null date", 0);
        }

        for (int i = 0; i < dateFormatArr.length; i++)
        {
            try
            {
                return dateFormatArr[i].parse(data);

            } catch (ParseException e)
            {

            }
        }
        throw new ParseException("Unable to parse date:" + data, 0);
    }

    public Date parseTime(String data) throws ParseException
    {
        if (data == null || data.trim().length() == 0)
        {
            throw new ParseException("Unable to parse null time", 0);
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
        throw new ParseException("Unable to parse time:" + data, 0);
    }

    public String extractSource(String tagname)
    {
        if (tagname.length() > 0)
        {
            int divider = tagname.indexOf('_');
            if (divider > 0)
            {
                String source = tagname.substring(0, divider);

                if (source.startsWith("PLC"))
                {
                    source = "PLC";
                }
                else  if (source.startsWith("RIT"))
                {
                    source = "RIT";
                }
                else  if (source.startsWith("ACS"))
                {
                    source = "ACS";
                }
                else  if (source.startsWith("MP"))
                {
                    source = "MP";
                }
                else  if (source.startsWith("SDI"))
                {
                    source = "SDI";
                }

                return source;
            } else
            {
                return "";
            }
        } else
        {
            return "";
        }

    }

    public String extractSubSource(String tagname)
    {
        if (tagname.length() > 0)
        {
            int divider = tagname.indexOf('_');
            if (divider > 0)
            {
                return tagname.substring(0, divider);
            } else
            {
                return "";
            }
        }
        return "";

    }

    public AlarmRecordBean parse(Map<String, Integer> columnMap, CSVLine csvLine) throws ParseException, InfoException, HeaderLineException,NoTagException
    {

        if (csvLine.getColumn(0).startsWith("i_ALM_ID"))
        {
            throw new HeaderLineException();
        } else if (csvLine.getColumn(0).startsWith("Data source is undefined:"))
        {
            throw new InfoException();
        } else if (csvLine.getLength() < 6)//likely an empty line ignore it
        {
            throw new InfoException();
        }

        Date alm_datelast = parseDate(getValue(columnMap, csvLine, i_ALM_DATELAST));
        Date alm_timelast = parseTime(getValue(columnMap, csvLine, i_ALM_TIMELAST));
        Date almTime = new Date(alm_datelast.getTime() + alm_timelast.getTime());

        String tagName = getValue(columnMap, csvLine, i_ALM_TAGNAME);

        AlarmRecordBean alarmRecord = new AlarmRecordBean();
        String alarmID = getValue(columnMap, csvLine, i_ALM_ID);
           
        alarmRecord.setAlarmTime(almTime);
        String sourceNode = getValue(columnMap, csvLine, i_ALM_LOGNODENAME);
        alarmRecord.setSystem(extractSource(tagName));
        alarmRecord.setSubSystem(extractSubSource(tagName));
        alarmRecord.setTagName(tagName);
        alarmRecord.setMessageType(getValue(columnMap, csvLine, i_ALM_MSGTYPE));
        alarmRecord.setPriority(getValue(columnMap, csvLine, i_ALM_ALMPRIORITY));
        alarmRecord.setStatus(getValue(columnMap, csvLine, i_ALM_ALMSTATUS));
        alarmRecord.setDescription(getValue(columnMap, csvLine, i_ALM_ALMEXTFLD2));

        try
        {    Integer.parseInt(alarmID);
        } catch (NumberFormatException e)
        {
            throw new ParseException("Alarm id \"" + alarmID + "\" must be a valid integer", 0);
        }

        if (alarmRecord.getTagName() == null || alarmRecord.getTagName().length() == 0)
        {
            throw new NoTagException(csvLine.toString());
        }
        else if(alarmRecord.getTagName().contains("DW1_MSG_") || alarmRecord.getTagName().contains("DW2_MSG_"))
        {
            throw new InfoException("Message Type: Drawworks operational message");
        }

        if (sourceNode != null && sourceNode.length() > 0)
        {
            if (sourceNode.startsWith("TERM") || sourceNode.startsWith("SERV"))
            {
                //good record
            } else
            {
                throw new ParseException("Source Node \"" + sourceNode + "\" must be a SERV* or TERM*", 0);
            }
        } else
        {
            throw new ParseException("Source Node cannot be null", 0);
        }

        //other message types exist but should be filtered out by this point
        if (alarmRecord.getMessageType() == null || alarmRecord.getMessageType().equalsIgnoreCase("EVENT"))
        {
            throw new InfoException("Message Type:" + alarmRecord.getMessageType());
        }
        if (!validateValues(alarmRecord.getMessageType(), validMessageType))
        {
            throw new ParseException("Invalid Message Type:" + alarmRecord.getMessageType(), 0);
        }

        if (alarmRecord.getPriority() == null)
        {
            throw new InfoException("No Priority:" + alarmRecord.getStatus());
        }

        if (!validateValues(alarmRecord.getPriority(), validPriortity))
        {
            throw new ParseException("Invalid Priority:" + alarmRecord.getPriority(), 0);
        }

        if (validateValues(alarmRecord.getStatus(), validStatusMessage))
        {
            throw new InfoException("Message Status:" + alarmRecord.getStatus());
        }

        if (!validateValues(alarmRecord.getStatus(), validStatus))
        {
            throw new ParseException("Invalid Status:" + alarmRecord.getStatus(), 0);
        }
        return alarmRecord;
    }

}
