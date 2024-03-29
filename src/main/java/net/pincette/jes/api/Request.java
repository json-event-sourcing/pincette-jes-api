package net.pincette.jes.api;

import static java.lang.Float.compare;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.api.Util.headersToString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.isFloat;
import static net.pincette.util.Util.tryToGetSilent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.json.JsonStructure;
import net.pincette.util.Array;
import net.pincette.util.Pair;

/**
 * Represents the part of an HTTP request that is relevant to the API.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class Request {

  /**
   * The request body.
   *
   * @since 1.0
   */
  public final JsonStructure body;

  /**
   * The cookies. If there are no cookies the map will be empty.
   *
   * @since 1.0
   */
  public final Map<String, String> cookies;

  /**
   * The request headers. If there are no headers the map will be empty.
   *
   * @since 1.0
   */
  public final Map<String, String[]> headers;

  /**
   * The request headers with the header names converted to lower case. If there are no headers the
   * map will be empty.
   *
   * @since 1.0
   */
  public final Map<String, String[]> headersLowerCaseKeys;

  /**
   * The user's preferred language tags, ordered by descending priority.
   *
   * @since 1.0
   */
  public final List<String> languages;

  /**
   * The HTTP method in upper case.
   *
   * @since 1.0
   */
  public final String method;

  /**
   * The full path of the request.
   *
   * @since 1.0
   */
  public final String path;

  /**
   * The query string of the URL.
   *
   * @since 1.0
   */
  public final Map<String, String[]> queryString;

  /**
   * The original URI of the request.
   *
   * @since 1.1
   */
  public final String uri;

  public Request() {
    this(null, null, null, null, null, null);
  }

  private Request(
      final String uri,
      final Map<String, String[]> headers,
      final String path,
      final String method,
      final JsonStructure body,
      final Map<String, String[]> queryString) {
    this.uri = uri;
    this.headers = headers != null ? headers : new HashMap<>();
    this.path = path;
    this.method = method != null ? method.toUpperCase() : null;
    this.body = body;
    this.queryString = queryString;
    headersLowerCaseKeys = toLowerCase(this.headers);
    languages = getLanguages(headersLowerCaseKeys);
    cookies = getCookies(headersLowerCaseKeys);
  }

  private static int compareWeighted(final Pair<String, Float> w1, final Pair<String, Float> w2) {
    return compare(w1.second, w2.second);
  }

  private static Map<String, String> getCookies(final Map<String, String[]> headers) {
    return Optional.ofNullable(headers.get("cookie"))
        .map(
            values ->
                stream(values)
                    .flatMap(value -> stream(value.split(";")))
                    .map(cookie -> cookie.trim().split("="))
                    .filter(split -> split.length == 2)
                    .collect(toMap(s -> s[0], s -> s[1])))
        .orElseGet(HashMap::new);
  }

  private static List<String> getLanguages(final Map<String, String[]> headers) {
    return Optional.ofNullable(headers.get("accept-language"))
        .map(Request::sortByQualityValue)
        .orElseGet(ArrayList::new);
  }

  private static Map<String, String[]> getQueryString(final String queryString) {
    return stream(queryString.split("&"))
        .map(parameter -> parameter.split("="))
        .filter(split -> split.length == 1 || split.length == 2)
        .map(split -> split.length == 1 ? new String[] {split[0], "true"} : split)
        .collect(
            toMap(
                s -> s[0],
                s -> new String[] {tryToGetSilent(() -> decode(s[1], UTF_8)).orElse(s[1])},
                Array::append));
  }

  private static float getWeight(final String s) {
    return Optional.of(s.split("="))
        .filter(split -> split.length == 2 && split[0].equalsIgnoreCase("q") && isFloat(split[1]))
        .map(split -> split[1])
        .map(Float::parseFloat)
        .filter(f -> f >= 0 || f <= 1)
        .orElse(0F);
  }

  private static List<String> sortByQualityValue(final String[] values) {
    return stream(values)
        .flatMap(v -> stream(v.split(" ,")))
        .map(v -> v.split(" ;"))
        .filter(split -> split.length == 1 || split.length == 2)
        .map(split -> pair(split[0], split.length == 1 ? 1 : getWeight(split[1])))
        .sorted(Request::compareWeighted)
        .map(pair -> pair.first)
        .toList();
  }

  private static <V> Map<String, V> toLowerCase(final Map<String, V> map) {
    return map.entrySet().stream()
        .collect(toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
  }

  private String queryStringToString() {
    return queryString.entrySet().stream()
        .flatMap(entry -> stream(entry.getValue()).map(v -> entry.getKey() + "=" + v))
        .collect(joining("&"));
  }

  public String toString() {
    return method
        + " "
        + path
        + (queryString != null ? ("?" + queryStringToString()) : "")
        + "\n"
        + headersToString(headers)
        + "\n"
        + (body != null ? string(body) : "");
  }

  public Request withBody(final JsonStructure body) {
    return new Request(uri, headers, path, method, body, queryString);
  }

  public Request withHeaders(final Map<String, String[]> headers) {
    return new Request(uri, headers, path, method, body, queryString);
  }

  public Request withMethod(final String method) {
    return new Request(uri, headers, path, method, body, queryString);
  }

  public Request withPath(final String path) {
    return new Request(uri, headers, path, method, body, queryString);
  }

  public Request withQueryString(final String queryString) {
    return new Request(
        uri, headers, path, method, body, queryString != null ? getQueryString(queryString) : null);
  }

  public Request withUri(final String uri) {
    return new Request(uri, headers, path, method, body, queryString);
  }
}
