package org.cron.job;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronScheduler {

	private static final Logger LOG = LoggerFactory
			.getLogger(CronScheduler.class);

	private ScheduledExecutorService executorService = Executors
			.newScheduledThreadPool(1);

	private final String name;
	private final UserTask taskWork;
	private String[] argv;


	/** Pattern to use for String representation of Dates/Times. */
	private final String dateTimeFormatPattern = "MM/dd/yyyy HH:mm:ss";
	final DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern(dateTimeFormatPattern);

	private volatile ScheduledFuture<?> scheduledTask = null;

	private AtomicInteger completedTasks = new AtomicInteger(0);

	public CronScheduler(String name, UserTask taskWork, String[] argv) {
		this.name = "Executor [" + name + "]";
		this.taskWork = taskWork;
		this.argv = argv;
	}

	public void start() {
		scheduleNextTask(doTaskWork(), true);
	}

	private Runnable doTaskWork() {
		return () -> {
			LOG.info(name + " [" + completedTasks.get() + "] start: "
					+ getCurrentZonedDateTime());
			try {
				taskWork.emptyProcessUserCommand();
				
			} catch (Exception ex) {
				LOG.error(name + " throw exception in " + getCurrentZonedDateTime(), ex);
			} 
			scheduleNextTask(doTaskWork(), false);
			LOG.info(name + " [" + completedTasks.get() + "] finish: "
					+ getCurrentZonedDateTime()+ " completed tasks: "
					+ completedTasks.incrementAndGet());
		};
	}

	private void scheduleNextTask(Runnable task, boolean firstTrigger) {
		
		long delay = computeNextDelay(firstTrigger);
		scheduledTask = executorService.schedule(task, delay, TimeUnit.SECONDS);
	}

	private long computeNextDelay(boolean isFirstTrigger) {
		int dayOfMonth = (argv.length>0)? Integer.parseInt(argv[0]):9;
    	int hourofDay = (argv.length>1)? Integer.parseInt(argv[1]):21;
    	int minOfHour=(argv.length>2)? Integer.parseInt(argv[2]):0;
    	
        Calendar runDate = Calendar.getInstance();
        runDate.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        runDate.set(Calendar.HOUR_OF_DAY, hourofDay);
        runDate.set(Calendar.MINUTE, minOfHour);
        
        int whichDay= runDate.get(Calendar.DAY_OF_WEEK);
		dayOfMonth = runDate.getActualMaximum(Calendar.DAY_OF_MONTH);
		switch(whichDay){
			case Calendar.SATURDAY:
				runDate.set(Calendar.DAY_OF_MONTH, dayOfMonth-1);
				break;
			case Calendar.SUNDAY:
				runDate.set(Calendar.DAY_OF_MONTH, dayOfMonth-2);
				break;
		}
        
        if(isFirstTrigger) {
        	
        	 runDate.add(Calendar.MONTH, 0);//set to next month
        }else {
        	 runDate.add(Calendar.MONTH, 1);//set to next month
        }
        Calendar cal = Calendar.getInstance();
        long delay = (runDate.getTimeInMillis()-cal.getTimeInMillis())/1000;
        LOG.info(name + " make schedule at "+ showTime(runDate.getTimeInMillis()) +" from current time "+  showTime(cal.getTimeInMillis())+" after delay  "+delay);
        return delay;
	}

	public ZonedDateTime getCurrentZonedDateTime() {
		return ZonedDateTime.now();
	}
	
    private String showTime(long milliSeconds) {
    	Calendar runDate = Calendar.getInstance();
    	runDate.setTimeInMillis(milliSeconds);
        SimpleDateFormat df2 = new SimpleDateFormat("MMM-dd-yyyy HH:mm:ss");
        return df2.format(runDate.getTime());
    }
	public static void main(String...argv){
		
		String dateTime = "10/15/2017 21:49:00";
		
		CronScheduler scheduler = new CronScheduler("JOB", new UserTask("dir###ls -lrt"),argv );
		scheduler.start();
	}
}