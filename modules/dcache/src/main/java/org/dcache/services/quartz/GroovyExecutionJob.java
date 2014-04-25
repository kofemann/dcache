package org.dcache.services.quartz;

import groovy.lang.GroovyShell;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroovyExecutionJob implements Job {

    public static final String SCRIPT_PROP = "script";
    private final static Logger _log = LoggerFactory.getLogger(GroovyExecutionJob.class);

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        String scriptFileName = (String) jec.getJobDetail().getJobDataMap().get(SCRIPT_PROP);
        try {
            File scriptFile = new File(scriptFileName);
            if (!scriptFile.exists()) {
                throw new FileNotFoundException(scriptFileName);
            }
            GroovyShell shell = new GroovyShell();
            Object script = shell.evaluate(scriptFile);

            Method m = script.getClass().getMethod("execute", JobExecutionContext.class);
            m.invoke(script, jec);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            _log.error("Failed to execute script {} : {}", scriptFileName, cause == null? e.toString() : cause.toString());
            throw new JobExecutionException(cause == null? e : cause);
        }
    }
}
