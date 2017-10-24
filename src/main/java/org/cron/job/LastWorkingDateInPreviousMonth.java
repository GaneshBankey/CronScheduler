package org.cron.job;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class LastWorkingDateInPreviousMonth {

	final static SimpleDateFormat runDateformatter = new SimpleDateFormat("MM/dd/yyyy");
	final static SimpleDateFormat oracleDateformatter = new SimpleDateFormat("dd-MMM-yyyy");
	public static void main(String... argv){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
		int whichDay= cal.get(Calendar.DAY_OF_WEEK);
		int dayOfMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		switch(whichDay){
			case Calendar.SATURDAY:
					cal.set(Calendar.DAY_OF_MONTH, dayOfMonth-1);
				break;
			case Calendar.SUNDAY:
					cal.set(Calendar.DAY_OF_MONTH, dayOfMonth-2);
				break;
		}
		System.out.println(runDateformatter.format(cal.getTime()));
		System.out.println(oracleDateformatter.format(cal.getTime()));
	}
	
}
