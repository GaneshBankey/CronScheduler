package org.cron.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserTask  {
	private static final Logger LOG = LoggerFactory
			.getLogger(UserTask.class);
	public String[] commands;
	public CommandExecutor commandExector = new CommandExecutor();
	public UserTask(String commands){
		this.commands = commands.split("###");
	}
	
	public void processUserCommand() {
		for(String command: commands){
			LOG.info("Submitted User Task Command : "+ command);
			commandExector.execute(command);
		}
		
	}
	public void emptyProcessUserCommand(){
		LOG.info("Submitted User Task Command : ");
	}
}
