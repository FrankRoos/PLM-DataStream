package org.gft.adapters.plm;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class HttpConfig {

    private final String model;
    private final String username;
    private final String password;
    private final String signal_name;
    private String lowest_date;
    private final String highest_date;


    public HttpConfig(String username, String password, String model, String signal_name, String lowest_date, String highest_date) {
        this.username = username;
        this.password = password;
        this.model = model;
        this.signal_name = signal_name;
        this.lowest_date = lowest_date;
        this.highest_date = highest_date;

    }

    public String getRepository(){
        return "TruePLMprojectsRep";
    }
    public String getBaseUrl(){ return "https://kyklos.jotne.com/EDMtruePLM/api/"; }
    public String getGroup(){
        return "sdai-group";
    }
    public String getUsername() {
        return this.username;
    }
    public String getPassword() {
        return this.password;
    }
    public String getModel() {  return this.model;}
    public String getSignal() {  return this.signal_name;}

    public String getHighestDate(){
        return getMillis(highest_date);
    }
     /*public String getLowestDate(){
        return getMillis(lowest_date);
    }
    public void setLowestDate(String old_date){
        this.lowest_date = old_date;
    }*/

    private String getMillis(String date){
        String timestamp = null;
        try{
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date myDate = dateFormat.parse(date);
           timestamp = String.valueOf(myDate.getTime());
        }catch (ParseException e){
            e.printStackTrace();
        }
        return timestamp;
    }

    public String CurrentDateTime(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Current Time = "+dtf.format(now));
        return getMillis(dtf.format(now));
    }

    public String LastDateTime() throws java.text.ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = " ";
        try{
            Date myDate = dateFormat.parse(this.lowest_date);
            // convert date to localdatetime
            LocalDateTime local_date_time = myDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            local_date_time = local_date_time.plusMinutes(5);
            Date date_plus = Date.from(local_date_time.atZone(ZoneId.systemDefault()).toInstant());
            date = dateFormat.format(date_plus);
            this.lowest_date = date;
        }catch (ParseException e){
            e.printStackTrace();
        }
        // convert LocalDateTime to date
        System.out.println("Last Time = "+date);
        return getMillis(date);
    }
}
