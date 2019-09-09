package net.pincette.jes.api;

import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;

import java.util.HashMap;
import java.util.Map;
import javax.json.JsonObject;
import org.reactivestreams.Publisher;

/**
 * The immutable response of a request.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Response {
  public static final String CONTENT_TYPE = "application/json";

  /**
   * The response body.
   *
   * @since 1.0
   */
  public final Publisher<JsonObject> body;

  /**
   * The response headers. When there are no headers the map will be empty.
   *
   * @since 1.0
   */
  public final Map<String, String[]> headers;

  /**
   * The HTTP status code.
   *
   * @since 1.0
   */
  public final int statusCode;

  private Response(final int statusCode) {
    this(statusCode, null, null);
  }

  private Response(
      final int statusCode, final Map<String, String[]> headers, final Publisher<JsonObject> body) {
    this.statusCode = statusCode;
    this.headers = headers != null ? headers : new HashMap<>();
    this.body = body;
  }

  public static Response accepted() {
    return new Response(202);
  }

  public static Response badRequest() {
    return new Response(400);
  }

  public static Response created() {
    return new Response(201);
  }

  public static Response forbidden() {
    return new Response(403);
  }

  public static Response notAuthorized() {
    return new Response(401);
  }

  public static Response notFound() {
    return new Response(404);
  }

  public static Response notImplemented() {
    return new Response(501);
  }

  public static Response ok() {
    return new Response(200);
  }

  public static Response redirect(final String location) {
    return new Response(303, map(pair("Location", new String[] {location})), null);
  }

  public Response withBody(final Publisher<JsonObject> body) {
    return new Response(statusCode, headers, body);
  }

  public Response withHeaders(final Map<String, String[]> headers) {
    return new Response(statusCode, headers, body);
  }
}
