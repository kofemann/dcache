package org.dcache.util.jetty;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.Cookie;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpFields.Mutable;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

import static org.mockito.BDDMockito.given;

public class RateLimitedHandlerListTest {

/*
    private RateLimitedHandlerList handlerList;

    @Before
    public void setUp() throws Exception {

        var config = new RateLimitedHandlerList.Configuration();
        config.setMaxClientsToTrack(100);
        config.setGlobalRequestsPerSecond(100);
        config.setNumErrorsBeforeBlocking(3);
        config.setLimitPercentagePerClient(25);
        config.setClientIdleTime(10);
        config.setClientIdleTimeUnit(ChronoUnit.MINUTES);
        config.setClientBlockingTime(10);
        config.setClientBlockingTimeUnit(ChronoUnit.SECONDS);
        config.setErrorCountingWindow(20);
        config.setErrorCountingWindowUnit(ChronoUnit.SECONDS);

        handlerList = new RateLimitedHandlerList(config);

        handlerList.addHandler(new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                try {
                    // fake some work otherwise the rate limiter will block right away
                    Thread.sleep(10);
                    callback.succeeded();
                } catch (InterruptedException e) {
                    callback.failed(e);
                    throw new RuntimeException(e);
                }
                return true;
            }
        });
        handlerList.start();
        //handlerList.setMaxGlobalRequestsPerSecond(10);
    }

    @Test
    public void testGlobalRateOk() throws Exception {

        Request request = Mockito.mock(Request.class);

        String[] ips = IntStream.range(2,21).mapToObj(i -> "127.1.1." + i).toArray(String[]::new);
        given(request.getRemoteAddr()).willReturn("127.1.1.1", ips);

        Response response = new SimpleResponse();

        for (int i = 0; i < 20; i++) {
            response.setStatus(0);
            handlerList.handle(request, response, Callback.NOOP);
        }

        assertEquals(0, response.getStatus());
    }


    @Test
    public void testGlobalRateExceeded() throws Exception {

        handlerList.setMaxGlobalRequestsPerSecond(10);

        Request request = Mockito.mock(Request.class);


        String[] ips = IntStream.range(2,21).mapToObj(i -> "127.1.1." + i).toArray(String[]::new);
        given(request.getConnectionMetaData().getRemoteSocketAddress()).willReturn("127.1.1.1", ips);
        Response response = new SimpleResponse();

        for (int i = 0; i < 20; i++) {
            handlerList.handle(request, response, Callback.NOOP);
        }

        assertEquals(HttpStatus.TOO_MANY_REQUESTS_429, response.getStatus());
    }


    @Test
    public void testBlockBadAuth() throws ServletException, IOException, InterruptedException {

        Handler badAuthHandler = new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
                response.setStatus(HttpStatus.UNAUTHORIZED_401);
                baseRequest.setHandled(true);
            }
        };
        handlerList.addHandler(badAuthHandler);


        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        given(request.getRemoteAddr()).willReturn("127.1.1.1");
        HttpServletResponse response = new SimpleResponse();

        for (int i = 0; i < 11; i++) {
            Request baseRequest = new Request(null, null);
            response.setStatus(0);
            handlerList.handle("/foo", baseRequest, request, response);
        }

        assertEquals(HttpStatus.TOO_MANY_REQUESTS_429, response.getStatus());

        given(request.getRemoteAddr()).willReturn("127.1.1.2");
        response.setStatus(0);
        Request baseRequest = new Request(null, null);
        handlerList.handle("/foo", baseRequest, request, response);

        assertEquals(HttpStatus.UNAUTHORIZED_401, response.getStatus());
    }



*/
    private static class SimpleResponse implements Response {

        int status;

        @Override
        public Request getRequest() {
            return null;
        }

        @Override
        public int getStatus() {
            return status;
        }

        @Override
        public void setStatus(int code) {
            this.status = code;
        }

        @Override
        public Mutable getHeaders() {
            return null;
        }

        @Override
        public Supplier<HttpFields> getTrailersSupplier() {
            return null;
        }

        @Override
        public void setTrailersSupplier(Supplier<HttpFields> trailers) {

        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public boolean hasLastWrite() {
            return false;
        }

        @Override
        public boolean isCompletedSuccessfully() {
            return false;
        }

        @Override
        public void reset() {

        }

        @Override
        public CompletableFuture<Void> writeInterim(int status, HttpFields headers) {
            return null;
        }

        @Override
        public void write(boolean last, ByteBuffer byteBuffer, Callback callback) {

        }
    }
}