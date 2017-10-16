package org.cron.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor {
	private static final Logger LOG = LoggerFactory
			.getLogger(CommandExecutor.class);
	public boolean execute(String command) {
		int statusCode = 0;
		Process p;
		try {
			LOG.info("Start Executing command: "+command);
			p = Runtime.getRuntime().exec(command);
			System.out.println("Waiting for batch file ...");
			statusCode = p.waitFor();
			LOG.info("Finished Executing command: "+command);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
		return statusCode==0? true:false;
	}
}
