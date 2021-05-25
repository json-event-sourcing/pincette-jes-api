package net.pincette.jes.api;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.reactivestreams.client.MongoClients.create;
import static io.jsonwebtoken.Jwts.parserBuilder;
import static java.lang.String.join;
import static java.lang.String.valueOf;
import static java.net.URLDecoder.decode;
import static java.security.KeyFactory.getInstance;
import static java.time.Instant.now;
import static java.util.Base64.getDecoder;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.api.Response.accepted;
import static net.pincette.jes.api.Response.badRequest;
import static net.pincette.jes.api.Response.forbidden;
import static net.pincette.jes.api.Response.internalServerError;
import static net.pincette.jes.api.Response.notAuthorized;
import static net.pincette.jes.api.Response.notFound;
import static net.pincette.jes.api.Response.notImplemented;
import static net.pincette.jes.api.Response.ok;
import static net.pincette.jes.api.Response.redirect;
import static net.pincette.jes.util.Commands.DELETE;
import static net.pincette.jes.util.Commands.PATCH;
import static net.pincette.jes.util.Commands.PUT;
import static net.pincette.jes.util.JsonFields.ACL;
import static net.pincette.jes.util.JsonFields.ACL_GET;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.JWT_BREAKING_THE_GLASS;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.ROLES;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.jes.util.Mongo.NOT_DELETED;
import static net.pincette.json.JsonUtil.addIf;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Collection.find;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Source.of;
import static net.pincette.util.Array.hasPrefix;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import java.io.Closeable;
import java.net.URI;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
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
import net.pincette.jes.util.AuditFields;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.BsonUtil;
import net.pincette.util.Collections;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jasypt.encryption.pbe.PBEStringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.StringFixedIvGenerator;
import org.jasypt.salt.StringFixedSaltGenerator;

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
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Server implements Closeable {
  private static final String ACCESS_TOKEN = "access_token";
  private static final String ERROR = "ERROR";
  private static final String MATCH = "$match";
  private static final int RESULT_SET_BUFFER = 100;
  private static final String SSE = "sse";
  private static final String SSE_SETUP = "sse-setup";
  private static final String UNKNOWN = "UNKNOWN";

  private String auditTopic;
  private boolean breakingTheGlass;
  private String[] contextPath = new String[0];
  private ElasticCommonSchema ecs;
  private String environment;
  private char[] fanoutSecret;
  private int fanoutTimeout = 20;
  private String fanoutUri;
  private JwtParser jwtParser;
  private String logTopic;
  private Logger logger = getGlobal();
  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private String mongoDatabaseName;
  private KafkaProducer<String, JsonObject> producer;
  private BiPredicate<JsonObject, JsonObject> responseFilter = (json, jwt) -> true;
  private String serviceVersion;

  private static Bson aclQuery(final JsonObject jwt) {
    return or(not(exists(ACL)), in(ACL + "." + ACL_GET, getRoles(jwt)));
  }

  private static JsonObject completeCommand(final JsonObject command) {
    return createObjectBuilder(command)
        .add(ID, command.getString(ID).toLowerCase())
        .add(TIMESTAMP, now().toEpochMilli())
        .add(
            CORR,
            Optional.ofNullable(command.getString(CORR, null))
                .orElseGet(() -> randomUUID().toString()))
        .build();
  }

  private static JsonObject createAuditRecord(
      final JsonObject jwt, final Path path, final String command) {
    return addIf(
            createObjectBuilder()
                .add(AuditFields.TYPE, path.fullType())
                .add(AuditFields.COMMAND, command)
                .add(AuditFields.TIMESTAMP, now().toEpochMilli())
                .add(
                    AuditFields.USER,
                    Optional.ofNullable(jwt.getString(SUB, null)).orElse("anonymous"))
                .add(AuditFields.BREAKING_THE_GLASS, jwt.getBoolean(JWT_BREAKING_THE_GLASS, false)),
            () -> path.id != null,
            b -> b.add(AuditFields.AGGREGATE, path.id))
        .build();
  }

  private static Optional<JsonObjectBuilder> createCommand(
      final String command, final JsonObject jwt, final Path path) {
    return Optional.ofNullable(path.id)
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
        .collect(toList());
  }

  private static Optional<String> getBearerToken(final Request request) {
    return tryWith(() -> getBearerTokenFromAuthorization(request))
        .or(() -> getBearerTokenFromQueryString(request))
        .or(() -> request.cookies.get(ACCESS_TOKEN))
        .get()
        .flatMap(t -> tryToGet(() -> decode(t, "UTF-8")));
  }

  private static String getBearerTokenFromAuthorization(final Request request) {
    return Optional.ofNullable(request.headersLowerCaseKeys.get("authorization"))
        .filter(values -> values.length == 1)
        .map(values -> values[0])
        .map(header -> header.split(" "))
        .filter(s -> s.length == 2)
        .filter(s -> s[0].equalsIgnoreCase("Bearer"))
        .map(s -> s[1])
        .orElse(null);
  }

  private static String getBearerTokenFromQueryString(final Request request) {
    return Optional.ofNullable(request.queryString)
        .map(q -> q.get(ACCESS_TOKEN))
        .filter(values -> values.length == 1)
        .map(values -> values[0])
        .orElse(null);
  }

  private static Optional<String> getCorr(final Request request) {
    return Optional.ofNullable(request.body)
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
            Optional.ofNullable(jwt.getJsonArray(ROLES))
                .map(
                    a ->
                        a.stream()
                            .filter(JsonUtil::isString)
                            .map(JsonUtil::asString)
                            .map(JsonString::getString))
                .orElseGet(Stream::empty),
            Stream.of(jwt.getString(SUB)))
        .collect(toSet());
  }

  private static RSAPublicKey getRSAPublicKey(final String key) {
    return tryToGetRethrow(
            () ->
                (RSAPublicKey)
                    getInstance("RSA")
                        .generatePublic(new X509EncodedKeySpec(getDecoder().decode(key))))
        .orElse(null);
  }

  private static Optional<String> getScheme(final Request request) {
    return getUri(request).map(URI::getScheme);
  }

  private static Optional<String> getTopLevelDomain(final Request request) {
    return getDomain(request).map(domain -> getExtension(domain).orElse(domain));
  }

  private static Optional<URI> getUri(final Request request) {
    return Optional.ofNullable(request.uri).flatMap(uri -> tryToGetSilent(() -> new URI(uri)));
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

  private static JsonObject onBehalfOf(final JsonObject jwt, final Request request) {
    return Optional.of(jwt.getString(SUB))
        .filter(sub -> sub.equals("system"))
        .map(sub -> request.headers.get("X-Pincette-JES-OnBehalfOf"))
        .filter(value -> value.length == 1)
        .map(value -> value[0])
        .filter(value -> value.length() > 0)
        .flatMap(JsonUtil::from)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .orElse(jwt);
  }

  public void close() {
    producer.close();

    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  private String commandTopic(final JsonObject command) {
    return command.getString(TYPE) + "-command" + (environment != null ? ("-" + environment) : "");
  }

  private BsonDocument completeMatch(final Bson original, final JsonObject jwt) {
    return new BsonDocument(MATCH, toBsonDocument(completeQuery(original, jwt)));
  }

  private Bson completeQuery(final Bson original, final JsonObject jwt) {
    return and(
        create(() -> list(NOT_DELETED))
            .updateIf(
                l ->
                    !jwt.getString(SUB).equals("system")
                        && (!breakingTheGlass || !jwt.getBoolean(JWT_BREAKING_THE_GLASS, false)),
                l -> l.add(aclQuery(jwt)))
            .updateIf(l -> original != null, l -> l.add(original))
            .build());
  }

  private List<BsonDocument> completeQuery(final List<BsonDocument> stages, final JsonObject jwt) {
    final List<BsonDocument> result =
        stages.stream()
            .map(
                stage ->
                    stage.containsKey(MATCH) ? completeMatch(stage.getDocument(MATCH), jwt) : stage)
            .collect(toList());

    return hasMatch(result)
        ? result
        : concat(Stream.of(completeMatch(null, jwt)), result.stream()).collect(toList());
  }

  private JsonObject createLogMessage(
      final Request request, final Response response, final Instant started, final JsonObject jwt) {
    final Instant ended = now();
    final String method = request.method != null ? request.method : UNKNOWN;
    final String user = Optional.ofNullable(jwt.getString(SUB, null)).orElse("anonymous");

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
                    .addIf(
                        () -> Optional.ofNullable(response.exception), ErrorBuilder::addThrowable)
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
    return tryWithLog(() -> getEncryptor().decrypt(username));
  }

  private CompletionStage<Response> delete(final JsonObject jwt, final Path path) {
    return createCommand(DELETE, jwt, path)
        .map(JsonObjectBuilder::build)
        .map(this::sendCommand)
        .orElseGet(() -> completedFuture(notFound()));
  }

  private String encodeUsername(final String username) {
    return tryWithLog(() -> getEncryptor().encrypt(username)).orElse(null);
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
    return tryWith(() -> path.sse && hasFanout() ? getSse(jwt) : null)
        .or(() -> path.sseSetup && hasFanout() ? getSseSetup(request) : null)
        .or(
            () ->
                Optional.ofNullable(mongoDatabase)
                    .map(database -> path.id != null ? getOne(jwt, path) : getList(jwt, path))
                    .orElse(null))
        .get()
        .orElseGet(() -> completedFuture(notImplemented()));
  }

  private MongoCollection<Document> getCollection(final Path path) {
    return mongoDatabase.getCollection(
        path.fullType() + (environment != null ? ("-" + environment) : ""));
  }

  private PBEStringEncryptor getEncryptor() {
    final StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();

    // Salt and IV are fixed because of load balancing, where /sse and /sse-setup may reach
    // different instances.

    encryptor.setSaltGenerator(new StringFixedSaltGenerator("dfg76dfb87df6g87dcv6d76f7x6tcvu7tsd"));
    encryptor.setIvGenerator(new StringFixedIvGenerator("dsfsdf8678s6df7dsf5sf5f56sf6sd5f76s5sdh"));
    encryptor.setPasswordCharArray(fanoutSecret);
    encryptor.setStringOutputType("hexadecimal");

    return encryptor;
  }

  @SuppressWarnings("java:S5659") // That is the responsibility of the user of the class.
  private Optional<JsonObject> getJwt(final Request request) {
    return getBearerToken(request)
        .flatMap(jwt -> tryToGetSilent(() -> jwtParser.parse(jwt).getBody()))
        .map(Claims.class::cast)
        .map(JsonUtil::from)
        .filter(jwt -> jwt.containsKey(SUB))
        .map(jwt -> onBehalfOf(jwt, request))
        .map(
            j ->
                SideEffect.<JsonObject>run(() -> logger.log(FINEST, "{0}", string(j)))
                    .andThenGet(() -> j));
  }

  private CompletionStage<Response> getList(final JsonObject jwt, final Path path) {
    return sendAudit(jwt, path, "list")
        .thenApply(
            r ->
                ok().withBody(
                        with(getCollection(path)
                                .find(completeQuery((Bson) null, jwt), BsonDocument.class))
                            .buffer(RESULT_SET_BUFFER)
                            .map(BsonUtil::fromBson)
                            .filter(json -> responseFilter.test(json, jwt))
                            .get()));
  }

  private CompletionStage<Response> getOne(final JsonObject jwt, final Path path) {
    final Function<JsonObject, Response> filter =
        json -> responseFilter.test(json, jwt) ? ok().withBody(of(list(json))) : forbidden();

    return find(getCollection(path), completeQuery(eq(ID, path.id), jwt), BsonDocument.class, null)
        .thenComposeAsync(
            result ->
                result.isEmpty()
                    ? completedFuture(notFound())
                    : sendAudit(jwt, path, "get")
                        .thenApply(r -> filter.apply(fromBson(result.get(0)))));
  }

  private Optional<Path> getPath(final Request request) {
    return Optional.ofNullable(request.path)
        .map(path -> new Path(path, contextPath))
        .filter(path -> path.valid);
  }

  private CompletionStage<Response> getSse(final JsonObject jwt) {
    final String username = jwt.getString(SUB);
    final String uri = createFanoutUri(fanoutUri, contextPath) + "?u=" + encodeUsername(username);

    logger.log(INFO, "Redirect to {0} for user {1}", new Object[] {uri, username});

    return completedFuture(redirect(uri));
  }

  private CompletionStage<Response> getSseSetup(final Request request) {
    return Optional.ofNullable(request.queryString)
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
    switch (request.method) {
      case "DELETE":
        return delete(jwt, path);
      case "GET":
        return get(request, jwt, path);
      case "PATCH":
        return patch(request, jwt, path);
      case "POST":
        return post(request, jwt, path);
      case "PUT":
        return put(request, jwt, path);
      default:
        return completedFuture(notImplemented());
    }
  }

  private boolean hasFanout() {
    return fanoutSecret != null && fanoutUri != null;
  }

  private boolean isCorrectObject(final Request request, final String id) {
    return Optional.ofNullable(request.body)
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
    final Runnable emit =
        () ->
            runAsync(
                () ->
                    producer.send(
                        new ProducerRecord<>(
                            logTopic,
                            randomUUID().toString(),
                            createLogMessage(request, response, started, jwt)),
                        (m, e) -> {}));

    final Supplier<Response> tryWithBody =
        () ->
            response.body != null
                ? new Response(
                    response.statusCode,
                    response.headers,
                    with(response.body)
                        .after(() -> SideEffect.<JsonObject>run(emit).andThenGet(() -> null))
                        .get(),
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
    return Optional.ofNullable(path.id)
        .map(
            id ->
                Optional.ofNullable(request.body)
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
    return Optional.ofNullable(path.id)
        .map(
            id ->
                isCorrectObject(request, id)
                    ? sendCommand(
                        createObjectBuilder(request.body.asJsonObject()).add(JWT, jwt).build())
                    : completedFuture(badRequest()))
        .orElseGet(() -> search(request, jwt, path));
  }

  private CompletionStage<Response> put(
      final Request request, final JsonObject jwt, final Path path) {
    return Optional.ofNullable(path.id)
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
                                    .exceptionally(
                                        e ->
                                            SideEffect.<Response>run(
                                                    () -> logger.log(SEVERE, ERROR, e))
                                                .andThenGet(
                                                    () -> internalServerError().withException(e)))
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

  private CompletionStage<Response> search(
      final Request request, final JsonObject jwt, final Path path) {
    return Optional.ofNullable(request.body)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(stages -> pair(fromJson(stages), string(stages)))
        .map(pair -> pair(completeQuery(pair.first, jwt), pair.second))
        .map(
            pair ->
                sendAudit(jwt, path, pair.second)
                    .thenApply(
                        r ->
                            ok().withBody(
                                    with(getCollection(path)
                                            .aggregate(pair.first, BsonDocument.class))
                                        .buffer(RESULT_SET_BUFFER)
                                        .map(BsonUtil::fromBson)
                                        .filter(json -> responseFilter.test(json, jwt))
                                        .get())))
        .orElseGet(() -> completedFuture(badRequest()));
  }

  private CompletionStage<Boolean> sendAudit(
      final JsonObject jwt, final Path path, final String command) {
    final String key = path.id != null ? path.id : path.fullType();

    return auditTopic != null
        ? send(
                producer,
                new ProducerRecord<>(auditTopic, key, createAuditRecord(jwt, path, command)))
            .thenApply(result -> must(result, r -> r))
        : completedFuture(true);
  }

  private CompletionStage<Response> sendCommand(final JsonObject command) {
    final JsonObject c = completeCommand(command);

    return send(producer, new ProducerRecord<>(commandTopic(c), c.getString(ID), c))
        .thenApply(result -> must(result, r -> r))
        .thenApply(result -> accepted());
  }

  private <T> Optional<T> tryWithLog(final SupplierWithException<T> supplier) {
    return tryToGet(
        supplier,
        e -> SideEffect.<T>run(() -> logger.log(SEVERE, ERROR, e)).andThenGet(() -> null));
  }

  /**
   * Causes all read-side requests to yield an entry in the audit trail, which is a Kafka topic.
   * Note that the write-side is already audited in pincette-jes.
   *
   * @param auditTopic the given Kafka topic. It may be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withAudit(final String auditTopic) {
    this.auditTopic = auditTopic;

    return this;
  }

  /**
   * Enables the breaking the glass feature. When the JSON Web Token of a request has the field
   * <code>breakingTheGlass</code> field set, the ACL is overruled and the request is let through.
   * Use this with auditing turned on.
   *
   * @return The server object itself.
   * @since 1.0
   */
  public Server withBreakingTheGlass() {
    breakingTheGlass = true;

    return this;
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
   * This is the secret used to encrypt the username in the Server-Sent Event set-up with the
   * fanout.io service. It must not be <code>null</code>.
   *
   * @param fanoutSecret the given secret.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withFanoutSecret(final String fanoutSecret) {
    this.fanoutSecret = fanoutSecret.toCharArray();

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
   * The public key with which all JSON Web Tokens are validated.
   *
   * @param key the given public key. It must not be <code>null</code>.
   * @return The server object itself.
   * @since 1.0
   */
  public Server withJwtPublicKey(final String key) {
    jwtParser = parserBuilder().setSigningKey(getRSAPublicKey(key)).build();

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
