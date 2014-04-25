package org.dcache.services.quartz;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import static org.quartz.JobKey.jobKey;

public class QuartzScheduler {

    private final Scheduler scheduler;

    public QuartzScheduler() throws SchedulerException {
        scheduler = StdSchedulerFactory.getDefaultScheduler();
    }

    public void init() throws SchedulerException {
        scheduler.start();
    }

    public void shutdown() throws SchedulerException {
        scheduler.shutdown();
    }

    public void add(JobDetail job, Trigger trigger) throws SchedulerException {
        scheduler.scheduleJob(job, trigger);
    }

    public boolean delete(String jobName) throws SchedulerException {
        return scheduler.deleteJob(jobKey(jobName));
    }

    public List<String> list() throws SchedulerException {
        List<String> jobs = new ArrayList<>();
        for (String groupName : scheduler.getJobGroupNames()) {

            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
                Date nextFireTime = triggers.get(0).getNextFireTime();

                jobs.add(jobName + " - " + nextFireTime);

            }
        }
        return jobs;
    }
}
