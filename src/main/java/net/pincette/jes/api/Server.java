package net.pincette.jes.api;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.lang.String.join;
import static java.lang.String.valueOf;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Base64.getDecoder;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.Commands.DELETE;
import static net.pincette.jes.Commands.PATCH;
import static net.pincette.jes.Commands.PUT;
import static net.pincette.jes.JsonFields.ACL;
import static net.pincette.jes.JsonFields.ACL_GET;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.OPS;
import static net.pincette.jes.JsonFields.ROLES;
import static net.pincette.jes.JsonFields.SUB;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.jes.api.Response.accepted;
import static net.pincette.jes.api.Response.badRequest;
import static net.pincette.jes.api.Response.forbidden;
import static net.pincette.jes.api.Response.internalServerError;
import static net.pincette.jes.api.Response.notAuthorized;
import static net.pincette.jes.api.Response.notFound;
import static net.pincette.jes.api.Response.notImplemented;
import static net.pincette.jes.api.Response.ok;
import static net.pincette.jes.api.Response.redirect;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Collection.find;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Source.of;
import static net.pincette.util.Array.hasPrefix;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetSilent;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.io.Closeable;
import java.net.URI;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.function.SupplierWithException;
import net.pincette.jes.elastic.ElasticCommonSchema;
import net.pincette.jes.elastic.ElasticCommonSchema.Builder;
import net.pincette.jes.elastic.ElasticCommonSchema.ErrorBuilder;
import net.pincette.jes.elastic.ElasticCommonSchema.EventBuilder;
import net.pincette.jes.elastic.ElasticCommonSchema.UrlBuilder;
import net.pincette.json.JsonUtil;
import net.pincette.jwt.Signer;
import net.pincette.jwt.Verifier;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.mongo.BsonUtil;
import net.pincette.util.Collections;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * This class handles all the HTTP logic for the JSON Event Sourcing library. This makes it easier
 * to use different HTTP server solutions. The URL path for an aggregate always has the form <code>
 * [/context]/app/type[/id]</code>. When the ID is present an individual aggregate instance is
 * addressed, otherwise the complete list of aggregates of the given type is addressed. It may be
 * limited with MongoDB search criteria. For this a POST should be used with a JSON array of staging
 * objects.
 *
 * <p>When aggregates change because of a command the result is sent back through the Kafka reply
 * topic. It is possible to push this to the client using Server-Sent Events. This is done with the
 * fanout.io service. The endpoint <code>/sse</code> is where the client should connect to. It will
 * be redirected to the fanout.io service with the encrypted username in a URL parameter. Then
 * fanout.io comes back to the <code>/sse-setup</code> endpoint, where the channel is created.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class Server implements Closeable {
  private static final Bson ACL_NOT_EXISTS = exists(ACL, false);
  private static final String AUTHORIZATION = "authorization";
  private static final String BEARER = "Bearer";
  private static final String ERROR = "ERROR";
  private static final String JWT_PAYLOAD_HEADER = "x-jwtpayload";
  private static final String MATCH = "$match";
  private static final String NO_ACL = "noacl";
  private static final int RESULT_SET_BUFFER = 10;
  private static final String SSE = "sse";
  private static final String SSE_SETUP = "sse-setup";
  private static final String UNKNOWN = "UNKNOWN";

  private String[] contextPath = new String[0];
  private ElasticCommonSchema ecs;
  private String environment;
  private Signer fanoutSigner;
  private Verifier fanoutVerifier;
  private int fanoutTimeout = 20;
  private String fanoutUri;
  private String logTopic;
  private Logger logger = getGlobal();
  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private String mongoDatabaseName;
  private KafkaProducer<String, JsonObject> producer;
  private boolean producerOwner;
  private BiPredicate<JsonObject, JsonObject> responseFilter = (json, jwt) -> true;
  private String serviceVersion;

  private static Bson aclQuery(final JsonObject jwt, final boolean noAcl) {
    return noAcl ? ACL_NOT_EXISTS : or(ACL_NOT_EXISTS, in(ACL + "." + ACL_GET, getRoles(jwt)));
  }

  private static JsonObject completeCommand(final JsonObject command) {
    return createObjectBuilder(command)
        .add(ID, command.getString(ID).toLowerCase())
        .add(TIMESTAMP, now().toEpochMilli())
        .add(
            CORR,
            ofNullable(command.getString(CORR, null)).orElseGet(() -> randomUUID().toString()))
        .build();
  }

  private static Optional<JsonObjectBuilder> createCommand(
      final String command, final JsonObject jwt, final Path path) {
    return ofNullable(path.id)
        .map(
            id ->
                createObjectBuilder()
                    .add(COMMAND, command)
                    .add(JWT, jwt)
                    .add(ID, id)
                    .add(TYPE, path.fullType()));
  }

  private static String createFanoutUri(final String fanoutUri, final String[] contextPath) {
    return fanoutUri
        + (contextPath.length > 0 ? ("/" + join("/", contextPath)) : "")
        + "/"
        + SSE_SETUP;
  }

  private static JsonObject createPutCommand(final Request request, final JsonObject jwt) {
    return createObjectBuilder(request.body.asJsonObject()).add(JWT, jwt).add(COMMAND, PUT).build();
  }

  private static List<BsonDocument> fromJson(final JsonArray array) {
    return array.stream()
        .filter(JsonUtil::isObject)
        .map(BsonUtil::fromJson)
        .map(BsonValue::asDocument)
        .toList();
  }

  private static Optional<String> getCorr(final Request request) {
    return ofNullable(request.body)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(json -> json.getString(CORR, null))
        .map(String::toLowerCase);
  }

  private static Optional<String> getDomain(final Request request) {
    return getUri(request)
        .map(URI::getAuthority)
        .map(authority -> pair(authority, authority.indexOf('@')))
        .map(pair -> pair.second != -1 ? pair.first.substring(pair.second + 1) : pair.first);
  }

  private static Optional<String> getExtension(final Request request) {
    return getUriPath(request).flatMap(Server::getExtension);
  }

  private static Optional<String> getExtension(final String s) {
    return Optional.of(s.lastIndexOf('.')).filter(i -> i != -1).map(i -> s.substring(i + 1));
  }

  private static Optional<String> getFragment(final Request request) {
    return getUri(request).map(URI::getFragment);
  }

  private static Optional<Integer> getPort(final Request request) {
    return getUri(request).map(URI::getPort).filter(port -> port != -1);
  }

  private static Optional<String> getQuery(final Request request) {
    return getUri(request).map(URI::getQuery);
  }

  private static Set<String> getRoles(final JsonObject jwt) {
    return concat(
            ofNullable(jwt.getJsonArray(ROLES)).stream()
                .flatMap(
                    a ->
                        a.stream()
                            .filter(JsonUtil::isString)
                            .map(JsonUtil::asString)
                            .map(JsonString::getString)),
            Stream.of(jwt.getString(SUB)))
        .collect(toSet());
  }

  private static Optional<String> getScheme(final Request request) {
    return getUri(request).map(URI::getScheme);
  }

  private static Optional<String> getTopLevelDomain(final Request request) {
    return getDomain(request).map(domain -> getExtension(domain).orElse(domain));
  }

  private static Optional<URI> getUri(final Request request) {
    return ofNullable(request.uri).flatMap(uri -> tryToGetSilent(() -> new URI(uri)));
  }

  private static Optional<String> getUriPath(final Request request) {
    return getUri(request).map(URI::getPath);
  }

  private static boolean hasMatch(final List<BsonDocument> stages) {
    return stages.stream().anyMatch(stage -> stage.containsKey(MATCH));
  }

  private static <T> T log(final Logger logger, final T obj) {
    logger.log(FINEST, "{0}", obj);

    return obj;
  }

  private static boolean noAcl(final Request request) {
    return ofNullable(request)
        .map(r -> singleValueQueryParameter(r, NO_ACL))
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  private static String singleValueQueryParameter(final Request request, final String name) {
    return ofNullable(request.queryString)
        .map(q -> q.get(name))
        .filter(values -> values.length == 1)
        .map(values -> values[0])
        .orElse(null);
  }

  public void close() {
    if (producerOwner) {
      producer.close();
    }

    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  private String commandTopic(final JsonObject command) {
    return command.getString(TYPE) + "-command" + (environment != null ? ("-" + environment) : "");
  }

  private BsonDocument completeMatch(
      final Bson original, final JsonObject jwt, final boolean noAcl) {
    return new BsonDocument(MATCH, toBsonDocument(completeQuery(original, jwt, noAcl)));
  }

  private Bson completeQuery(final Bson original, final JsonObject jwt, final boolean noAcl) {
    return ofNullable(original)
        .map(o -> !jwt.getString(SUB).equals("system") ? and(o, aclQuery(jwt, noAcl)) : o)
        .orElseGet(Filters::empty);
  }

  private List<BsonDocument> completeQuery(
      final List<BsonDocument> stages, final JsonObject jwt, final boolean noAcl) {
    final List<BsonDocument> result =
        stages.stream()
            .map(
                stage ->
                    stage.containsKey(MATCH)
                        ? completeMatch(stage.getDocument(MATCH), jwt, noAcl)
                        : stage)
            .toList();

    return hasMatch(result)
        ? result
        : concat(Stream.of(completeMatch(null, jwt, noAcl)), result.stream()).toList();
  }

  private JsonObject createLogMessage(
      final Request request, final Response response, final Instant started, final JsonObject jwt) {
    final Instant ended = now();
    final String method = request.method != null ? request.method : UNKNOWN;
    final String user = ofNullable(jwt.getString(SUB, null)).orElse("anonymous");

    return ecs()
        .builder()
        .addUser(user)
        .addIf(() -> getCorr(request), Builder::addTrace)
        .addEvent()
        .addAction(method)
        .addDataset(getPath(request).map(Path::fullType).orElse(UNKNOWN))
        .addDuration(ended.toEpochMilli() - started.toEpochMilli())
        .addCreated(started)
        .addStart(started)
        .addEnd(ended)
        .addIf(b -> response.statusCode >= 400, EventBuilder::addFailure)
        .build()
        .addIf(
            b -> response.statusCode >= 400 || response.exception != null,
            b ->
                b.addError()
                    .addCode(valueOf(response.statusCode))
                    .addIf(() -> ofNullable(response.exception), ErrorBuilder::addThrowable)
                    .build())
        .addHttp()
        .addMethod(method)
        .addStatusCode(response.statusCode)
        .addIf(b -> request.body != null, b -> b.addRequestBodyContent(string(request.body)))
        .build()
        .addUrl()
        .addUsername(user)
        .addIf(b -> request.uri != null, b -> b.addFull(request.uri).addOriginal(request.uri))
        .addIf(() -> getDomain(request), UrlBuilder::addDomain)
        .addIf(() -> getExtension(request), UrlBuilder::addExtension)
        .addIf(() -> getFragment(request), UrlBuilder::addFragment)
        .addIf(() -> getPort(request), UrlBuilder::addPort)
        .addIf(() -> getQuery(request), UrlBuilder::addQuery)
        .addIf(() -> getScheme(request), UrlBuilder::addScheme)
        .addIf(() -> getTopLevelDomain(request), UrlBuilder::addTopLevelDomain)
        .addIf(() -> getUriPath(request), UrlBuilder::addPath)
        .build()
        .build();
  }

  private Optional<String> decodeUsername(final String username) {
    return tryWithLog(() -> fanoutVerifier.verify(username.replace(' ', '+')))
        .flatMap(decoded -> decoded.map(DecodedJWT::getSubject));
  }

  private CompletionStage<Response> delete(final JsonObject jwt, final Path path) {
    return createCommand(DELETE, jwt, path)
        .map(JsonObjectBuilder::build)
        .map(this::sendCommand)
        .orElseGet(() -> completedFuture(notFound()));
  }

  private String encodeUsername(final String username) {
    return tryWithLog(
            () ->
                encode(
                    fanoutSigner.sign(
                        com.auth0
                            .jwt
                            .JWT
                            .create()
                            .withClaim(SUB, username)
                            .withExpiresAt(now().plusSeconds(60))),
                    US_ASCII))
        .orElse(null);
  }

  private Response exception(final Throwable e) {
    return SideEffect.<Response>run(() -> logger.log(SEVERE, ERROR, e))
        .andThenGet(() -> internalServerError().withException(e));
  }

  private ElasticCommonSchema ecs() {
    return ecs != null
        ? ecs
        : SideEffect.<ElasticCommonSchema>run(
                () ->
                    ecs =
                        new ElasticCommonSchema()
                            .withApp(logger.getName())
                            .withService(logger.getName())
                            .withLogLevel(logger.getLevel())
                            .withEnvironment(environment)
                            .withServiceVersion(serviceVersion))
            .andThenGet(() -> ecs);
  }

  private Map<String, String[]> fanoutHeaders(final String username) {
    return map(
        pair("Content-Type", new String[] {"text/event-stream"}),
        pair("Cache-Control", new String[] {"no-cache"}),
        pair("Grip-Hold", new String[] {"stream"}),
        pair("Grip-Channel", new String[] {username}),
        pair(
            "Grip-Keep-Alive", new String[] {":\\n\\n; format=cstring; timeout=" + fanoutTimeout}));
  }

  private CompletionStage<Response> get(
      final Request request, final JsonObject jwt, final Path path) {
    return tryWith(() -> ofNullable(path.sse && hasFanout() ? getSse(jwt) : null))
        .or(() -> ofNullable(path.sseSetup && hasFanout() ? getSseSetup(request) : null))
        .or(
            () ->
                ofNullable(mongoDatabase)
                    .map(
                        database ->
                            path.id != null ? getOne(jwt, path) : completedFuture(notFound())))
        .get()
        .orElseGet(() -> completedFuture(notImplemented()));
  }

  private static Optional<String> getBearerToken(final Request request) {
    return Optional.of(request.headersLowerCaseKeys)
        .map(h -> h.get(AUTHORIZATION))
        .filter(h -> h.length == 1)
        .map(h -> h[0])
        .map(header -> header.split(" "))
        .filter(s -> s.length == 2)
        .filter(s -> s[0].equalsIgnoreCase(BEARER))
        .map(s -> s[1]);
  }

  private MongoCollection<Document> getCollection(final Path path) {
    return mongoDatabase.getCollection(
        path.fullType() + (environment != null ? ("-" + environment) : ""));
  }

  private Optional<JsonObject> getJwt(final Request request) {
    return getBearerToken(request).flatMap(Server::getJwtPayload);
  }

  private static Optional<JsonObject> getJwtPayload(final String token) {
    return Optional.of(token)
        .map(t -> t.split("\\."))
        .filter(s -> s.length > 1)
        .map(s -> s[1].replace('-', '+').replace('_', '/'))
        .map(s -> getDecoder().decode(s))
        .map(b -> new String(b, UTF_8))
        .flatMap(JsonUtil::from)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject);
  }

  private CompletionStage<Response> getOne(final JsonObject jwt, final Path path) {
    final Function<JsonObject, Response> filter =
        json -> responseFilter.test(json, jwt) ? ok().withBody(of(list(json))) : forbidden();

    return find(
            getCollection(path),
            completeQuery(eq(ID, path.id), jwt, false),
            BsonDocument.class,
            null)
        .thenApply(result -> result.isEmpty() ? notFound() : filter.apply(fromBson(result.get(0))));
  }

  private Optional<Path> getPath(final Request request) {
    return ofNullable(request.path)
        .map(path -> new Path(path, contextPath))
        .filter(path -> path.valid);
  }

  private Response getResults(
      final List<BsonDocument> aggregation,
      final JsonObject jwt,
      final Path path,
      final boolean noAcl) {
    return ok().withBody(
            with(toFlowPublisher(
                    getCollection(path)
                        .aggregate(completeQuery(aggregation, jwt, noAcl), BsonDocument.class)))
                .buffer(RESULT_SET_BUFFER)
                .map(BsonUtil::fromBson)
                .filter(json -> responseFilter.test(json, jwt))
                .get());
  }

  private CompletionStage<Response> getSse(final JsonObject jwt) {
    final String username = jwt.getString(SUB);
    final String uri = createFanoutUri(fanoutUri, contextPath) + "?u=" + encodeUsername(username);

    logger.log(INFO, "Redirect to {0} for user {1}", new Object[] {uri, username});

    return completedFuture(redirect(uri));
  }

  private CompletionStage<Response> getSseSetup(final Request request) {
    return ofNullable(request.queryString)
        .map(q -> q.get("u"))
        .filter(u -> u.length == 1)
        .flatMap(u -> decodeUsername(u[0]))
        .map(
            username ->
                SideEffect.<CompletionStage<Response>>run(
                        () -> logger.log(INFO, "Set up fanout channel for user {0}", username))
                    .andThenGet(() -> completedFuture(ok().withHeaders(fanoutHeaders(username)))))
        .orElseGet(() -> completedFuture(forbidden()));
  }

  private CompletionStage<Response> handleRequest(
      final Request request, final JsonObject jwt, final Path path) {
    return switch (request.method) {
      case "DELETE" -> delete(jwt, path);
      case "GET" -> get(request, jwt, path);
      case "PATCH" -> patch(request, jwt, path);
      case "POST" -> post(request, jwt, path);
      case "PUT" -> put(request, jwt, path);
      default -> completedFuture(notImplemented());
    };
  }

  private boolean hasFanout() {
    return fanoutSigner != null && fanoutVerifier != null && fanoutUri != null;
  }

  private boolean isCorrectObject(final Request request, final String id) {
    return ofNullable(request.body)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(json -> json.containsKey(ID) && json.containsKey(TYPE))
        .map(
            json ->
                id.equalsIgnoreCase(json.getString(ID))
                    && getPath(request)
                        .map(path -> path.fullType().equals(json.getString(TYPE)))
                        .orElse(false))
        .orElse(false);
  }

  private Response log(
      final Request request, final Response response, final Instant started, final JsonObject jwt) {
    final Runnable emit = () -> sendLog(request, response, started, jwt);
    final Supplier<Response> tryWithBody =
        () ->
            response.body != null
                ? new Response(
                    response.statusCode,
                    response.headers,
                    with(response.body).map(probe(emit)).get(),
                    response.exception)
                : SideEffect.<Response>run(emit).andThenGet(() -> response);

    return logTopic != null && logger.getLevel().intValue() <= INFO.intValue()
        ? tryWithBody.get()
        : response;
  }

  private void openDatabase() {
    if (mongoDatabase == null && mongoClient != null && mongoDatabaseName != null) {
      mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    }
  }

  private CompletionStage<Response> patch(
      final Request request, final JsonObject jwt, final Path path) {
    return ofNullable(path.id)
        .map(
            id ->
                ofNullable(request.body)
                    .filter(JsonUtil::isArray)
                    .flatMap(
                        body ->
                            createCommand(PATCH, jwt, path)
                                .map(builder -> builder.add(OPS, body).build()))
                    .map(this::sendCommand)
                    .orElseGet(() -> completedFuture(badRequest())))
        .orElseGet(() -> completedFuture(notFound()));
  }

  private CompletionStage<Response> post(
      final Request request, final JsonObject jwt, final Path path) {
    return ofNullable(path.id)
        .map(
            id ->
                isCorrectObject(request, id)
                    ? sendCommand(
                        createObjectBuilder(request.body.asJsonObject()).add(JWT, jwt).build())
                    : completedFuture(badRequest()))
        .orElseGet(() -> completedFuture(search(request, jwt, path)));
  }

  private CompletionStage<Response> put(
      final Request request, final JsonObject jwt, final Path path) {
    return ofNullable(path.id)
        .map(
            id ->
                isCorrectObject(request, id)
                    ? sendCommand(createPutCommand(request, jwt))
                    : completedFuture(badRequest()))
        .orElseGet(() -> completedFuture(notFound()));
  }

  /**
   * Processes a request asynchronously. It must always have a JSON Web Token, which can be a bearer
   * token in the <code>Authorization</code> header, the URL parameter <code>access_token</code> or
   * a cookie with the name <code>access_token</code>.
   *
   * @param request the given request.
   * @return The completion stage for the response.
   * @since 1.0
   */
  public CompletionStage<Response> request(final Request request) {
    final Instant started = now();

    return getPath(log(logger, request))
        .map(
            path ->
                path.sseSetup
                    ? getSseSetup(request)
                    : getJwt(request)
                        .filter(jwt -> jwt.containsKey(SUB))
                        .map(
                            jwt ->
                                handleRequest(request, jwt, path)
                                    .exceptionally(this::exception)
                                    .thenApply(response -> log(request, response, started, jwt)))
                        .orElseGet(() -> completedFuture(notAuthorized())))
        .orElseGet(() -> completedFuture(notFound()))
        .thenApply(response -> log(logger, response));
  }

  /**
   * Indicates whether a request would return more than one JSON object. The client can use this to
   * prepare an array or not without complicating streaming.
   *
   * @param request the given request
   * @return Returns <code>true</code> if the result will contain more than one JSON object.
   * @since 1.0
   */
  public boolean returnsMultiple(final Request request) {
    return (request.method.equals("GET") || request.method.equals("POST"))
        && getPath(request).map(path -> path.id == null).orElse(false);
  }

  private Response search(final Request request, final JsonObject jwt, final Path path) {
    return ofNullable(request.body)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(stages -> getResults(fromJson(stages), jwt, path, noAcl(request)))
        .orElseGet(Response::badRequest);
  }

  private CompletionStage<Response> sendCommand(final JsonObject command) {
    final JsonObject c = completeCommand(command);

    return send(producer, new ProducerRecord<>(commandTopic(c), c.getString(ID), c))
        .thenApply(result -> must(result, r -> r))
        .thenApply(result -> accepted());
  }

  private void sendLog(
      final Request request, final Response response, final Instant started, final JsonObject jwt) {
    runAsync(
        () ->
            producer.send(
                new ProducerRecord<>(
                    logTopic,
                    randomUUID().toString(),
                    createLogMessage(request, response, started, jwt)),
                (m, e) -> {}));
  }

  private <T> Optional<T> tryWithLog(final SupplierWithException<T> supplier) {
    return tryToGet(
        supplier,
        e -> SideEffect.<T>run(() -> logger.log(SEVERE, ERROR, e)).andThenGet(() -> null));
  }

  /**
   * Sets the URL context path.
   *
   * @param contextPath the given context path. It may be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withContextPath(final String contextPath) {
    this.contextPath =
        contextPath != null ? getSegments(contextPath, "/").toArray(String[]::new) : new String[0];

    return this;
  }

  /**
   * This value is used as a suffix to the name of the command Kafka topic. Several environment such
   * as "dev", "test", "acceptance", etc. could live in the same Kafka cluster.
   *
   * @param environment the given environment. It may be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withEnvironment(final String environment) {
    this.environment = environment;

    return this;
  }

  /**
   * This is the private key used to sign the JWT containing the username in the Server-Sent Event
   * set-up with the fanout.io service. It must not be <code>null</code>.
   *
   * @param privateKey the given private key in PEM format.
   * @return The server object itself.
   * @since 2.0
   */
  public Server withFanoutPrivateKey(final String privateKey) {
    this.fanoutSigner = new Signer(privateKey);

    return this;
  }

  /**
   * This is the private key used to sign the JWT containing the username in the Server-Sent Event
   * set-up with the fanout.io service. It must not be <code>null</code>.
   *
   * @param privateKey the given private key.
   * @return The server object itself.
   * @since 2.0
   */
  public Server withFanoutPrivateKey(final PrivateKey privateKey) {
    this.fanoutSigner = new Signer(privateKey);

    return this;
  }

  /**
   * This is the public key used to verify the JWT containing the username in the Server-Sent Event
   * set-up with the fanout.io service. It must not be <code>null</code>.
   *
   * @param publicKey the given public key in PEM format.
   * @return The server object itself.
   * @since 2.0
   */
  public Server withFanoutPublicKey(final String publicKey) {
    this.fanoutVerifier = new Verifier(publicKey);

    return this;
  }

  /**
   * This is the public key used to verify the JWT containing the username in the Server-Sent Event
   * set-up with the fanout.io service. It must not be <code>null</code>.
   *
   * @param publicKey the given public key.
   * @return The server object itself.
   * @since 2.0
   */
  public Server withFanoutPublicKey(final PublicKey publicKey) {
    this.fanoutVerifier = new Verifier(publicKey);

    return this;
  }

  /**
   * Sets the amount of seconds a fanout.io connection is allowed to be idle.
   *
   * @param fanoutTimeout the timeout in seconds.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withFanoutTimeout(final int fanoutTimeout) {
    this.fanoutTimeout = fanoutTimeout;

    return this;
  }

  /**
   * The URL of the fanout.io service, which is used by the Server-Sent Events endpoint.
   *
   * @param fanoutUri the given URI. It may be <code>null</code>, in which case there will be no
   *     support for Server-Sent Events.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withFanoutUri(final String fanoutUri) {
    this.fanoutUri = fanoutUri;

    return this;
  }

  /**
   * The configuration for the Kafka producer, which is used to send commands to the command topic.
   *
   * @param config the given configuration. It must not be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withKafkaConfig(final Map<String, Object> config) {
    producer =
        createReliableProducer(
            Collections.put(config, "client.id", randomUUID().toString()),
            new StringSerializer(),
            new JsonSerializer());
    producerOwner = true;

    return this;
  }

  /**
   * The Kafka log topic. When set requests will be logged in the Elastic Common Schema.
   *
   * @param logTopic the given log topic.
   * @return The server object itself.
   * @see <a href="https://www.elastic.co/guide/en/ecs/current/ecs-field-reference.html">ECS Field
   *     Reference</a>
   * @since 1.1
   */
  public Server withLogTopic(final String logTopic) {
    this.logTopic = logTopic;

    return this;
  }

  /**
   * The logger.
   *
   * @param logger the given logger. It must not be <code>null</code>.
   * @return The server object itself.
   * @since 1.0.3
   */
  public Server withLogger(final Logger logger) {
    this.logger = logger;

    return this;
  }

  /**
   * The MongoDB database for the read-side.
   *
   * @param database the given database. It must not be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withMongoDatabase(final String database) {
    mongoDatabaseName = database;
    openDatabase();

    return this;
  }

  /**
   * The URI of the MongoDB service. When it is not set there will be no read-side.
   *
   * @param uri the given URI. It must not be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withMongoUri(final String uri) {
    mongoClient = create(uri);
    openDatabase();

    return this;
  }

  /**
   * The Kafka producer that will be used. If the <code>withKafkaConfig</code> method is used
   * instead, a private producer will be created.
   *
   * @param producer the given producer.
   * @return The server object itself.
   * @since 3.0
   */
  public Server withProducer(final KafkaProducer<String, JsonObject> producer) {
    this.producer = producer;
    producerOwner = false;

    return this;
  }

  /**
   * A function to transform the JSON objects on the read-side.
   *
   * @param responseFilter the given filter function. It may be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withResponseFilter(final BiPredicate<JsonObject, JsonObject> responseFilter) {
    this.responseFilter = responseFilter;

    return this;
  }

  /**
   * The service version. This will be used in ECS logging.
   *
   * @param serviceVersion the given service version.
   * @return The server object itself.
   * @since 1.1
   */
  public Server withServiceVersion(final String serviceVersion) {
    this.serviceVersion = serviceVersion;

    return this;
  }

  private static class Path {
    final String app;
    final String id;
    final boolean sse;
    final boolean sseSetup;
    final String type;
    final boolean valid;

    private Path(final String path, final String[] contextPath) {
      final String[] segments = getSegments(path, "/").toArray(String[]::new);
      final boolean aggregatePath = isAggregate(segments, contextPath);

      sse = isSse(segments, contextPath);
      sseSetup = isSseSetup(segments, contextPath);
      valid = aggregatePath || sse || sseSetup;
      app = aggregatePath ? segments[contextPath.length] : null;
      type = aggregatePath ? getType(segments[contextPath.length + 1]) : null;
      id =
          aggregatePath && segments.length == contextPath.length + 3
              ? segments[contextPath.length + 2].toLowerCase()
              : null;
    }

    private static String getType(final String type) {
      return Optional.of(type.indexOf('-'))
          .filter(i -> i != -1)
          .map(i -> type.substring(i + 1))
          .orElse(type);
    }

    private static boolean isAggregate(final String[] path, final String[] contextPath) {
      return (path.length == contextPath.length + 2 || path.length == contextPath.length + 3)
          && hasPrefix(path, contextPath);
    }

    private static boolean isSse(final String[] path, final String[] contextPath) {
      return path.length == contextPath.length + 1 && path[contextPath.length].equals(SSE);
    }

    private static boolean isSseSetup(final String[] path, final String[] contextPath) {
      return path.length == contextPath.length + 1 && path[contextPath.length].equals(SSE_SETUP);
    }

    private String fullType() {
      return app + "-" + type;
    }
  }
}
