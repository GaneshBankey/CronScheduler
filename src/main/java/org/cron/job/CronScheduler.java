package org.cron.job;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

	private String startDateAndTime;

	/** Pattern to use for String representation of Dates/Times. */
	private final String dateTimeFormatPattern = "MM/dd/yyyy HH:mm:ss";
	final DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern(dateTimeFormatPattern);

	private volatile ScheduledFuture<?> scheduledTask = null;

	private AtomicInteger completedTasks = new AtomicInteger(0);

	public CronScheduler(String name, UserTask taskWork, String startDateAndTime) {
		this.name = "Executor [" + name + "]";
		this.taskWork = taskWork;
		this.startDateAndTime   = startDateAndTime;

	}

	public void start() {
		scheduleNextTask(doTaskWork());
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
			scheduleNextTask(doTaskWork());
			LOG.info(name + " [" + completedTasks.get() + "] finish: "
					+ getCurrentZonedDateTime()+ " completed tasks: "
					+ completedTasks.incrementAndGet());
		};
	}

	private void scheduleNextTask(Runnable task) {
		LOG.info(name + " make schedule in " + getCurrentZonedDateTime());
		long delay = computeNextDelay();
		
		scheduledTask = executorService.schedule(task, delay, TimeUnit.SECONDS);
	}

	private long computeNextDelay() {
		ZonedDateTime zonedNow = getCurrentZonedDateTime();
		ZonedDateTime zonedNextTarget =getZonedNextTarget(zonedNow);
//			zonedNextTarget = zonedNextTarget.plusDays(1);
//		}
		LOG.info("Next "+ name + " has schedule in " + zonedNextTarget.format(formatter));
		Duration duration = Duration.between(zonedNow, zonedNextTarget);
		return duration.getSeconds();
	}

	public ZonedDateTime getCurrentZonedDateTime() {
		return ZonedDateTime.now();
	}
	public ZonedDateTime getZonedNextTarget(ZonedDateTime zonedNow){
		int second = zonedNow.getSecond()+5;
		int minute = zonedNow.getMinute();
		int hour = zonedNow.getHour();
		int day = zonedNow.getDayOfMonth();
		int month = zonedNow.getMonthValue();
		int year = zonedNow.getYear();
		if(second > 60){
			second = 0;
			minute +=1;
		}
		if(minute >60){
			minute = 0;
			hour +=1;
		}
		if(hour > 24){
			hour =0;
			day +=1;
		}
		
		
		
		return ZonedDateTime.of(year,month, day, hour, minute, second, 0, zonedNow.getZone());
	}
	public static void main(String...argv){
//		if(argv.length < 2 ){
//			LOG.error("Wrong Arguments: expected datetime and Daily/Weekly/Monthly");
//			System.out.println("Wrong Arguments: expected datetime and Daily/Weekly/Monthly");
//		}
		String dateTime = "10/15/2017 21:49:00";
		
		CronScheduler scheduler = new CronScheduler("JOB", new UserTask("dir###ls -lrt"),dateTime );
		scheduler.start();
	}
}