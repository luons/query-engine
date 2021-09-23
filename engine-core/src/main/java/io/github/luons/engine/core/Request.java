package io.github.luons.engine.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import org.apache.http.*;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;

import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Request<T extends HttpRequestBase> {

    private final T request;

    private final Client client;

    private final URIBuilder uriBuilder;

    public Request(T request, Client client, URI baseUrl) {
        this.request = request;
        this.client = client;
        uriBuilder = new URIBuilder(baseUrl);
    }

    public Request<T> jsonEntity(Object value) {
        return entity(client.jsonEntity(value));
    }

    public Request<T> urlEncodedEntity(List<? extends NameValuePair> value) {
        return entity(client.urlEncodedFormEntity(value));
    }

    public Request<T> entity(HttpEntity entity) {
        if (!(request instanceof HttpEntityEnclosingRequest)) {
            throw new IllegalArgumentException("HTTP " + request.getMethod() + " doesn't accept an entity");
        }
        ((HttpEntityEnclosingRequest) request).setEntity(entity);
        return this;
    }

    public Request<T> requestId(String requestId) {
        header(Client.REQUEST_ID, requestId);
        return this;
    }

    public Request<T> param(String name, Object value) {
        if (value != null) {
            uriBuilder.setParameter(name, value.toString());
        }
        return this;
    }

    public Request<T> addParam(String name, Object value) {
        if (value != null) {
            uriBuilder.addParameter(name, value.toString());
        }
        return this;
    }

    public Request<T> range(Long fromBytes, Long toBytes) {
        checkArgument((fromBytes != null || toBytes != null), ("Either fromBytes or toBytes must be specified"));
        checkArgument((fromBytes == null || fromBytes >= 0), ("Invalid fromBytes: " + fromBytes));
        checkArgument((toBytes == null || toBytes >= 0), ("Invalid toBytes: " + fromBytes));
        StringBuilder sb = new StringBuilder();
        if (fromBytes != null) {
            sb.append(fromBytes);
        }
        sb.append('-');
        if (toBytes != null) {
            sb.append(toBytes);
        }
        header(HttpHeaders.RANGE, sb.toString());
        return this;
    }

    public Request<T> path(Object... paths) {
        StringBuilder sb = append(new StringBuilder(), uriBuilder.getPath());
        for (Object path : paths) {
            sb = append(sb, path.toString());
        }
        uriBuilder.setPath(sb.toString());
        return this;
    }

    public Request<T> header(String name, Object value) {
        if (value != null) {
            request.setHeader(name, value.toString());
        }
        return this;
    }

    public long execute(OutputStream out) {
        return client.execute(httpRequest(), out);
    }

    public HttpResponse execute() {
        return client.execute(httpRequest());
    }

    public <R> R execute(TypeReference<R> typeReference) {
        return client.execute(httpRequest(), typeReference);
    }

    public <R> R execute(Class<R> type) {
        return client.execute(httpRequest(), type);
    }

    public <R> R execute(JavaType type) {
        return client.execute(httpRequest(), type);
    }

    public <R> R execute(ResponseHandler<R> handler) {
        return client.execute(httpRequest(), handler);
    }

    public T httpRequest() {
        URI uri = getRequestUri();
        request.setURI(uri);
        return request;
    }

    public URI getRequestUri() {
        try {
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Malformed URI '" + e.getInput() + "': " + e.getMessage());
        }
    }

    private static StringBuilder append(StringBuilder sb, String path) {
        if (path == null || path.length() == 0) {
            return sb;
        }
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (sb.length() == 0 || sb.charAt(sb.length() - 1) != '/') {
            sb.append('/');
        }
        return sb.append(path);
    }
}
