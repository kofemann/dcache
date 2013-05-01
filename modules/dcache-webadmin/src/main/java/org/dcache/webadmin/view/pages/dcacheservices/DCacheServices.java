package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.FeedbackPanel;

import org.dcache.webadmin.view.pages.basepage.BasePage;

/**
 * Main overview of all dCache-Services
 * @author jans
 */
public class DCacheServices extends BasePage {

    private static final long serialVersionUID = 6130566348881616211L;

    public DCacheServices() {
        add(new FeedbackPanel("feedback"));
        add(new Label("dCacheInstanceName",
                getWebadminApplication().getDcacheName()));
    }
}
