package org.cron.job;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class CronSchedulerTest {
	/** Pattern to use for String representation of Dates/Times. */
	private final String dateTimeFormatPattern = "MM/dd/yyyy HH:mm:ss";
	final DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern(dateTimeFormatPattern);
	private final ZonedDateTime now89 = ZonedDateTime.now();
	//private final ZonedDateTime now89 = ZonedDateTime.parse("12/13/1980 12:10:30", formatter);

	public static void main(String[] args) {
		CronSchedulerTest test = new CronSchedulerTest();
		test.demonstrateDateTimeFormatFormatting();
	}

	public void demonstrateDateTimeFormatFormatting() {
	
//		final String nowString = formatter.format(now89);
//		System.out.println(now89 + " formatted with DateTimeFormatter and '"
//				+ dateTimeFormatPattern + "': " + nowString);
//		System.out.println(now89.getDayOfMonth());
//		System.out.println(now89.getHour());
//		System.out.println(now89.getMinute());
//		System.out.println(now89.getSecond());
//		
		
		//Convert String to LocalDateTime
        String date = "2016-08-22 14:30:45";
        DateTimeFormatter format = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

        LocalDateTime ldt = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("LocalDateTime : " + format.format(ldt));
        
	}
}
