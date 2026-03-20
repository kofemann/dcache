package org.dcache.webdav;

import static com.google.common.base.Preconditions.checkArgument;


import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import jakarta.annotation.PostConstruct;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.CrossOriginHandler;

/**
 * A Jetty Handler that uses Jetty's CrossOriginFilter to implement support for CORS.
 */
// DROPME
public class CrossOriginResourceSharingHandler extends CrossOriginHandler {



}
