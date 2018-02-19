package org.dcache.poolmanager;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 *
 */
public class RequestContainerCli implements CellCommandListener, CellSetupProvider {

    /*
     * Old CLI compatibility.
     */
    @AffectsSetup
    @Command(name = "rc onerror")
    public class DummyCommand1 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc retry")
    public class DummyCommand2 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @Command(name = "xrc ls")
    public class DummyCommand3 implements Callable<Serializable> {

        @Argument()
        String args;

        @Override
        public Serializable call() throws Exception {
            return (Serializable) Collections.emptyList();
        }
    }

    @Command(name = "rc replicate")
    public class DummyCommand4 implements Callable<String> {

        @Argument(index = 0)
        String arg1;

        @Argument(index = 1)
        String arg2;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc set retry")
    public class DummyCommand5 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc set max retries")
    public class DummyCommand6 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc set warning path billing")
    public class DummyCommand7 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc set poolpingtimer")
    public class DummyCommand9 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "rc set max restore")
    public class DummyCommand10 implements Callable<String> {

        @Argument()
        String args;

        @Override
        public String call() throws Exception {
            return "";
        }
    }

}
