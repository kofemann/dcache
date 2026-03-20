/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 - 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.restful.util;

import com.google.common.base.Splitter;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * A class that accepts client GET requests and replies with static information.  The data is
 * static, but the representation is negotiable, based on the client-supplied preferences.  If the
 * client expresses no preference then the filename determines the format.
 */
public class StaticDataHandler extends Handler.Abstract {

    private static enum Media {
        JSON("application/json", "", "\n"),
        JAVASCRIPT("application/javascript", "var CONFIG = ", ";\n");

        private final String pre;
        private final String post;
        private final String mime;

        Media(String mime, String pre, String post) {
            this.mime = mime;
            this.pre = pre;
            this.post = post;
        }
    }

    private List<String> paths;
    private String json;

    @Autowired
    public void setPath(String path) {
        paths = Splitter.on(':').splitToList(path);
    }

    @Autowired
    public void setData(Map<String, String> data) {
        json = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(data);
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback)
          throws IOException {

        String target = request.getHttpURI().getPath();
        if (paths.contains(target)) {
            if (request.getMethod().equals("GET")) {
                handleRequest(request, response);
                callback.succeeded();
            } else {
                Response.writeError(request, response, callback, HttpStatus.METHOD_NOT_ALLOWED_405);
            }

            return true;
        }
        return false;
    }

    private Media decideMedia(String target, Request request) {

        List<String> types = request.getHeaders().getQualityCSV(HttpHeader.ACCEPT);
        for (String type : types) {
            switch (type) {
                case "application/json":
                case "text/json":
                    return Media.JSON;
                case "application/javascript":
                case "text/javascript":
                    return Media.JAVASCRIPT;
            }
        }

        int dot = target.lastIndexOf('.');
        if (dot != -1) {
            String extension = target.substring(dot + 1);
            switch (extension) {
                case "js":
                    return Media.JAVASCRIPT;
                case "json":
                    return Media.JSON;
            }
        }

        return Media.JAVASCRIPT;
    }

    private void handleRequest(Request request, Response response) {
        Media media = decideMedia(request.getHttpURI().getPath(), request);

        response.getHeaders().add(HttpHeader.CONTENT_TYPE, media.mime);
        response.getHeaders().add(HttpHeader.CONTENT_ENCODING, "UTF-8");
        response.getHeaders().add(HttpHeader.CONTENT_LENGTH, Integer.toString(media.pre.length() + json.length() + media.post.length()));

        try (PrintWriter writer = new PrintWriter(Response.asBufferedOutputStream(request, response))) {
            writer.print(media.pre);
            writer.print(json);
            writer.print(media.post);
            writer.flush();
        }
    }
}
