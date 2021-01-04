// $Id: RunSystem.java,v 1.7 2007-09-04 15:55:38 tigran Exp $

package diskCacheV111.util ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RunSystem {
    private static final Logger _log = LoggerFactory.getLogger(RunSystem.class);
    private static final Runtime __runtime = Runtime.getRuntime() ;
    private final String[] _exec ;
    private final int    _maxLines ;
    private final long   _timeout ;
    private final int     _id            = nextId() ;
    private Process _process ;
    private volatile boolean _processDone;
    private volatile boolean _interrupted;
    private final PrintWriter _errorPrintWriter   ;
    private final PrintWriter _outputPrintWriter  ;
    private final StringWriter _errorStringWriter ;
    private final StringWriter _outputStringWriter;

    private static int __counter = 100 ;
    private static synchronized int nextId(){ return __counter++ ; }

    public RunSystem(int maxLines, long timeout, String ... exec)
    {
        _exec     = exec ;
        _maxLines = maxLines ;
        _timeout  = timeout ;

        _outputStringWriter = new StringWriter() ;
        _errorStringWriter  = new StringWriter() ;
        _outputPrintWriter  = new PrintWriter( _outputStringWriter ) ;
        _errorPrintWriter   = new PrintWriter( _errorStringWriter ) ;
    }

    private void say(String str) {
        _log.debug("[{}] {}", _id, str);
    }

    public void go() throws IOException {
        _process = __runtime.exec(_exec);
        BufferedReader stdout = new BufferedReader(
                new InputStreamReader(_process.getInputStream()));
        BufferedReader stderr = new BufferedReader(
                new InputStreamReader(_process.getErrorStream()));

        /*
         * we do not need stdin of the process. To avoid file descriptor leaking close it.
         */
        _process.getOutputStream().close();

        try(stderr; stdout) {
            CompletableFuture<Void> readStdErr = CompletableFuture.runAsync(() -> runReader(stderr, _errorPrintWriter));
            CompletableFuture<Void> readStdOut = CompletableFuture.runAsync(() -> runReader(stdout, _outputPrintWriter));

            CompletableFuture<Void> process = CompletableFuture.runAsync(this::runProcess);

            CompletableFuture
                    .allOf(process, readStdErr, readStdOut)
                    .get(_timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ie) {
            if (!_processDone) {
                say("Master : Destroying process");
                _process.destroy();
            }
        }
        say("Master : Wait stopped : " + statusPrintout());
        if (!_processDone) {
            say("Master : Wait 2 loop : Destroying process");
            _process.destroy();
        }
    }
    private String statusPrintout(){
        return    ";interrupt="+_interrupted+
                  ";done="+_processDone;
    }

    public String getErrorString(){
       return _errorStringWriter.getBuffer().toString() ;
    }
    public String getOutputString(){
       return _outputStringWriter.getBuffer().toString() ;
    }
    public int getExitValue() throws IllegalThreadStateException {
       return _process.exitValue() ;
    }

    private void runProcess() {

        try {
            say("Process : waitFor called");
            int rr = _process.waitFor();
            say("Process : waitFor returned =" + rr + "; waiting for sync");
        } catch (InterruptedException ie) {
            _interrupted = true;
            say("Process : waitFor was interrupted ");
        } finally {
            _processDone = true;
            say("Process : done");
        }
    }
    private void runReader( BufferedReader in , PrintWriter out ){
        try {
            say("Reader started");
            in.lines().limit(_maxLines).forEach(out::println);
        }catch( Exception ioe ){
           say( "Reader Exception : "+ioe ) ;
        }finally{
           say( "Reader closing streams" ) ;
           try{ in.close() ; }catch(IOException e){}
           out.close() ;
        }
    }
    public static void main( String [] args ) throws Exception {
        if( args.length < 3 ){
            System.err.println( "Usage : ... <systemClass> <maxLines> <timeout>" ) ;
            System.exit(4) ;
        }
        long timeout = Long.parseLong( args[2] ) * 1000 ;
        int  maxLines = Integer.parseInt( args[1] ) ;

        RunSystem run = new RunSystem(maxLines, timeout, args[0] ) ;
        run.go() ;

        int rc = run.getExitValue() ;
        System.out.println("Exit Value : "+rc ) ;
        System.out.println("--------------- Output ------------" ) ;
        System.out.println( run.getOutputString() ) ;
        System.out.println("--------------- Error ------------" ) ;
        System.out.println( run.getErrorString() ) ;
        System.exit(rc) ;
    }

}
