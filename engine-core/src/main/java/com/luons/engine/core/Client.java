package com.luons.engine.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.luons.engine.core.exception.EngineException;
import com.luons.engine.core.spi.AuthorizationProvider;
import com.luons.engine.utils.HttpRequestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Formatter;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public abstract class Client {

    protected URI baseUrl;

    private String apiToken;

    protected final HttpClient httpClient;

    protected final HttpContext httpContext;

    protected final ObjectMapper objectMapper;

    protected AuthorizationProvider authorizationProvider;

    public static final String MESSAGE_FIELD = "message";

    public static final String ERROR_FIELD = "error";

    public static final String REQUEST_ID = "request_id";

    private static final String X_SERVICE_API_TOKEN = "X-Service-Api-Token";

    protected Client(ClientFactory factory) {
        this(factory, factory.baseUrl);
    }

    protected Client(ClientFactory factory, URI baseUrl) {
        httpClient = factory.httpClient;
        objectMapper = factory.objectMapper;
        authorizationProvider = factory.authorizationProvider;
        this.baseUrl = baseUrl;
        httpContext = new BasicHttpContext();
    }

    protected Client(ClientFactory factory, String serviceName) {
        this(factory, factory.baseUrl);
    }

    protected static boolean isApplicationJson(HttpEntity entity) {
        ContentType type = ContentType.get(entity);
        return type != null && ContentType.APPLICATION_JSON.getMimeType().equals(type.getMimeType());
    }

    protected static boolean isInformational(HttpResponse response) {
        return HttpRequestUtil.isInformational(response.getStatusLine().getStatusCode());
    }

    protected static boolean isSuccessful(HttpResponse response) {
        return HttpRequestUtil.isSuccessful(response.getStatusLine().getStatusCode());
    }

    protected static boolean isRedirection(HttpResponse response) {
        return HttpRequestUtil.isRedirection(response.getStatusLine().getStatusCode());
    }

    protected static boolean isClientError(HttpResponse response) {
        return HttpRequestUtil.isClientError(response.getStatusLine().getStatusCode());
    }

    protected static boolean isServerError(HttpResponse response) {
        return HttpRequestUtil.isServerError(response.getStatusLine().getStatusCode());
    }

    public Request<HttpGet> get(Object... paths) {
        return newRequest(new HttpGet(), paths);
    }

    public Request<HttpGet> get(String path) {
        String basePath = baseUrl.toString();
        URI uri = URI.create(basePath + path);
        return newRequest(new HttpGet(), uri);
    }

    public Request<HttpGet> get(URI uri, Object... paths) {
        return newRequest(new HttpGet(), uri, paths);
    }

    public Request<HttpHead> head(Object... paths) {
        return newRequest(new HttpHead(), paths);
    }

    public Request<HttpHead> head(URI uri, Object... paths) {
        return newRequest(new HttpHead(), uri, paths);
    }

    public Request<HttpPost> post(Object... paths) {
        return newRequest(new HttpPost(), paths);
    }

    public Request<HttpPost> post(URI uri, Object... paths) {
        return newRequest(new HttpPost(), uri, paths);
    }

    public Request<HttpPut> put(Object... paths) {
        return newRequest(new HttpPut(), paths);
    }

    public Request<HttpPut> put(URI uri, Object... paths) {
        return newRequest(new HttpPut(), uri, paths);
    }

    public Request<HttpPatch> patch(Object... paths) {
        return newRequest(new HttpPatch(), paths);
    }

    public Request<HttpPatch> patch(URI uri, Object... paths) {
        return newRequest(new HttpPatch(), uri, paths);
    }

    public Request<HttpDelete> delete(Object... paths) {
        return newRequest(new HttpDelete(), paths);
    }

    public Request<HttpDelete> delete(URI uri, Object... paths) {
        return newRequest(new HttpDelete(), uri, paths);
    }

    protected <T extends HttpRequestBase> Request<T> newRequest(T requestBase, Object... paths) {
        checkNotNull(baseUrl, ("baseUrl is null"));
        return newRequest(requestBase, baseUrl, paths);
    }

    protected <T extends HttpRequestBase> Request<T> newRequest(T requestBase, URI baseUrl, Object... paths) {
        checkNotNull(requestBase, ("requestBase is null"));
        checkNotNull(baseUrl, ("baseUrl is null"));
        return new Request<>(requestBase, (this), baseUrl).path(paths);
    }

    protected HttpEntity jsonEntity(Object value) {
        checkNotNull(value, ("value is null"));
        try {
            return new ByteArrayEntity(objectMapper.writeValueAsBytes(value), ContentType.APPLICATION_JSON);
        } catch (IOException e) {
            throw new EngineException("Error serializing JSON payload " + e);
        }
    }

    protected HttpEntity urlEncodedFormEntity(Iterable<? extends NameValuePair> params) {
        checkNotNull(params, ("params is null"));
        return new UrlEncodedFormEntity(params, Charsets.UTF_8);
    }

    protected HttpResponse execute(final HttpUriRequest request) {
        checkNotNull(request, ("request is null"));
        return execute(request, response -> {
            checkResponse(request, response);
            return response;
        });
    }

    protected long execute(final HttpUriRequest request, final OutputStream out) {
        checkNotNull(out, ("out is null"));
        return execute(request, response -> {
            checkResponse(request, response);
            return ByteStreams.copy(response.getEntity().getContent(), out);
        });
    }

    protected <T> T execute(HttpUriRequest req, TypeReference<T> typeReference) {
        return execute(req, typeFactory().constructType(typeReference));
    }

    protected <T> T execute(HttpUriRequest req, Class<T> type) {
        return execute(req, typeFactory().constructType(type));
    }

    protected <T> T execute(final HttpUriRequest request, final JavaType type) {
        checkNotNull(type, "type");
        return execute(request, response -> {
            checkResponse(request, response);
            if (response.getEntity() == null) {
                throw new EngineException("Unexpected empty response body: response = " + response.getStatusLine());
            }
            InputStream is = new BufferedInputStream(response.getEntity().getContent());
            return objectMapper.readValue(is, type);
        });
    }

    protected <T> T readJsonEntity(HttpResponse response, Class<T> type) throws IOException {
        HttpEntity entity = response.getEntity();
        return entity != null && isApplicationJson(entity) ? objectMapper.readValue(entity.getContent(), type) : null;
    }

    protected <T> T readJsonEntity(HttpResponse response, TypeReference<T> typeRef) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity != null && isApplicationJson(entity)) {
            return objectMapper.readValue(entity.getContent(), typeRef);
        }
        return null;
    }

    protected <T> T execute(HttpUriRequest request, ResponseHandler<T> handler) {
        checkNotNull(request, ("request is null"));
        checkNotNull(handler, ("handler is null"));
        prepareRequest(request);
        HttpResponse response;
        try {
            httpContext.removeAttribute(DefaultRedirectStrategy.REDIRECT_LOCATIONS);
            response = httpClient.execute(request, httpContext);
        } catch (IOException e) {
            throw new EngineException("Error executing request (url = " + request.getURI() + ")", e);
        }
        try {
            return handler.handleResponse(response);
        } catch (EngineException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineException("Error in response handler (url = " + request.getURI() + ")", e);
        } finally {
            postHandleResponse(response);
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    protected void postHandleResponse(HttpResponse response) {
        // Default implementation does nothing...
    }

    protected void prepareRequest(HttpUriRequest request) {
        // Accept JSON responses by default
        if (!request.containsHeader(HttpHeaders.ACCEPT)) {
            request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString());
        }
        // Add authorization header if specified
        if (!request.containsHeader(HttpHeaders.AUTHORIZATION)) {
            String authorization = getAuthorization();
            if (authorization != null) {
                request.setHeader(HttpHeaders.AUTHORIZATION, authorization);
            }
        }
        if (apiToken != null) {
            request.setHeader(X_SERVICE_API_TOKEN, apiToken);
        }
        // if (!request.containsHeader(REQUEST_ID)) {
        //     request.setHeader(REQUEST_ID, "requestId");
        // }
    }

    protected void checkResponse(HttpUriRequest request, HttpResponse response) {
        if (!isSuccessful(response)) {
            StatusLine status = response.getStatusLine();
            Formatter fmt = new Formatter().format("%s failed: %s", request.getMethod(), status);
            String message = extractErrorMessage(response);
            if (message != null) {
                fmt.format(" (%s)", message);
            }
            fmt.format(" (url = %s)", toLogSafeString(request.getURI()));
            throw new EngineException(status.getStatusCode(), fmt.toString());
        }
        Header header = response.getFirstHeader(HttpHeaders.AUTHORIZATION);
        if (header != null) {
            setAuthorization(header.getValue());
        }
    }

    protected String extractErrorMessage(HttpResponse response) {
        HttpEntity entity = response.getEntity();
        if (entity == null || entity.getContentLength() == 0) {
            return null;
        }
        try {
            String message = EntityUtils.toString(entity, Charsets.UTF_8);
            JsonNode node = objectMapper.readTree(message);
            if (node.has(MESSAGE_FIELD)) {
                return node.get(MESSAGE_FIELD).asText();
            } else if (node.has(ERROR_FIELD)) {
                return node.get(ERROR_FIELD).asText();
            }
            log.warn("Fail to extract error message from body " + message);
        } catch (IOException ex) {
            log.error("extractErrorMessage is exception " + ex);
        }
        return null;
    }

    protected TypeFactory typeFactory() {
        return objectMapper.getTypeFactory();
    }

    public URI getBaseUrl() {
        return baseUrl;
    }

    protected void setBaseUrl(URI baseUrl) {
        this.baseUrl = checkNotNull(baseUrl, "baseUrl");
    }

    public String getApiToken() {
        return apiToken;
    }

    public void setApiToken(String sharedToken) {
        this.apiToken = sharedToken;
    }

    public String getAuthorization() {
        return authorizationProvider != null ? authorizationProvider.getAuthorization() : null;
    }

    public void setAuthorization(final String authorization) {
        checkNotNull(authorization, ("authorization is null"));
        setAuthorizationProvider(new AuthorizationProvider() {
            @Override
            public String getAuthorization() {
                return authorization;
            }

            @Override
            public boolean refreshToken() {
                return false;
            }
        });
    }

    public void setAuthorizationProvider(AuthorizationProvider authorizationProvider) {
        this.authorizationProvider = authorizationProvider;
    }

    private static String toLogSafeString(URI uri) {
        String scheme = uri.getScheme();
        try {
            return new URI(scheme, (null), uri.getHost(), uri.getPort(), uri.getPath(), (null), (null)).toString();
        } catch (URISyntaxException e) {
            throw new AssertionError();
        }
    }
}
