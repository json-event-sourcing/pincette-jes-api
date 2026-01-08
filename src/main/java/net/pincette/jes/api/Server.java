package net.pincette.jes.api;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.time.Instant.now;
import static java.util.Arrays.copyOfRange;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.Commands.DELETE;
import static net.pincette.jes.Commands.PUT;
import static net.pincette.jes.JsonFields.ACL;
import static net.pincette.jes.JsonFields.ACL_GET;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.JWT;
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
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Collection.find;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Source.of;
import static net.pincette.util.Array.hasPrefix;
import static net.pincette.util.Cases.withValue;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.isUUID;
import static net.pincette.util.Util.must;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
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
  private static final Set<String> FORBIDDEN_STAGES =
      set(
          "$changeStream",
          "$changeStreamSplitLargeEvent",
          "$colStats",
          "$currentOp",
          "$graphLookup",
          "$indexStats",
          "$listLocalSessions",
          "$listSampledQueries",
          "$listSearchIndexes",
          "$merge",
          "$out",
          "$planCacheStats",
          "$querySettings",
          "$queryStats",
          "$shardedDataDistribution");
  private static final String LANGUAGE = "$language";
  private static final String LOOKUP = "$lookup";
  private static final String MATCH = "$match";
  private static final String MY_PRIVILEGES = "_myPrivileges";
  private static final String NO_ACL = "noacl";
  private static final int RESULT_SET_BUFFER = 10;
  private static final String SEARCH = "$search";
  private static final String TEXT = "$text";

  private String[] contextPath = new String[0];
  private String environment;
  private Logger logger = getGlobal();
  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private String mongoDatabaseName;
  private KafkaProducer<String, JsonObject> producer;
  private boolean producerOwner;
  private BiPredicate<JsonObject, JsonObject> responseFilter = (json, jwt) -> true;
  private boolean warnLookup;

  private static Bson aclQuery(final JsonObject jwt, final boolean noAcl) {
    return noAcl ? ACL_NOT_EXISTS : or(ACL_NOT_EXISTS, in(ACL + "." + ACL_GET, getRoles(jwt)));
  }

  private static Processor<JsonObject, JsonObject> addMyPrivileges(final JsonObject jwt) {
    return map(json -> addMyPrivileges(json, jwt));
  }

  private static JsonObject addMyPrivileges(final JsonObject json, final JsonObject jwt) {
    return ofNullable(json.getJsonObject(ACL))
        .flatMap(acl -> ofNullable(jwt.getJsonArray(ROLES)).map(roles -> myPrivileges(acl, roles)))
        .map(privileges -> createObjectBuilder(json).add(MY_PRIVILEGES, privileges).build())
        .orElse(json);
  }

  private static JsonObject completeCommand(final JsonObject command) {
    return createObjectBuilder(command)
        .add(ID, command.getString(ID).toLowerCase())
        .add(TIMESTAMP, now().toEpochMilli())
        .add(
            CORR,
            ofNullable(command.getString(CORR, null)).orElseGet(() -> randomUUID().toString()))
        .remove(MY_PRIVILEGES)
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

  private static boolean hasMatch(final List<BsonDocument> stages) {
    return stages.stream().anyMatch(stage -> stage.containsKey(MATCH));
  }

  private static <T> T log(final Logger logger, final T obj) {
    logger.log(FINEST, "{0}", obj);

    return obj;
  }

  private static JsonArray myPrivileges(final JsonObject acl, final JsonArray roles) {
    return acl.entrySet().stream()
        .filter(e -> isArray(e.getValue()))
        .filter(e -> !intersection(e.getValue().asJsonArray(), roles).isEmpty())
        .map(Entry::getKey)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  private static boolean noAcl(final Request request) {
    return ofNullable(request)
        .map(r -> singleValueQueryParameter(r, NO_ACL))
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  private static Optional<String> preferredLanguage(final Request request) {
    return request.languages.stream().findFirst();
  }

  private static String singleValueQueryParameter(final Request request, final String name) {
    return ofNullable(request.queryString)
        .map(q -> q.get(name))
        .filter(values -> values.length == 1)
        .map(values -> values[0])
        .orElse(null);
  }

  private static Stream<String> stages(final JsonArray stages) {
    return stages.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .flatMap(o -> o.keySet().stream());
  }

  // The buggers don't support all countries:
  // https://www.mongodb.com/docs/manual/reference/collation-locales-defaults/#supported-languages-and-locales
  private static String stripCountry(final String locale) {
    return Optional.of(locale.indexOf('-'))
        .filter(i -> i != -1)
        .map(i -> locale.substring(0, i))
        .orElse(locale);
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

    return hasMatch(result) || (!result.isEmpty() && result.getFirst().containsKey(SEARCH))
        ? result
        : concat(Stream.of(completeMatch(null, jwt, noAcl)), result.stream()).toList();
  }

  private CompletionStage<Response> delete(final JsonObject jwt, final Path path) {
    return createCommand(DELETE, jwt, path)
        .map(JsonObjectBuilder::build)
        .map(this::sendCommand)
        .orElseGet(() -> completedFuture(notFound()));
  }

  private Response exception(final Throwable e) {
    return SideEffect.<Response>run(() -> logger.log(SEVERE, ERROR, e))
        .andThenGet(() -> internalServerError().withException(e));
  }

  private CompletionStage<Response> get(final JsonObject jwt, final Path path) {
    return ofNullable(mongoDatabase)
        .map(database -> path.id != null ? getOne(jwt, path) : completedFuture(notFound()))
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
    return getBearerToken(request).flatMap(net.pincette.jwt.Util::getJwtPayload);
  }

  private CompletionStage<Response> getOne(final JsonObject jwt, final Path path) {
    final Function<JsonObject, Response> filter =
        json ->
            responseFilter.test(json, jwt)
                ? ok().withBody(with(of(list(json))).map(addMyPrivileges(jwt)).get())
                : forbidden();

    return find(
            getCollection(path),
            completeQuery(eq(ID, path.id), jwt, false),
            BsonDocument.class,
            null)
        .thenApply(
            result -> result.isEmpty() ? notFound() : filter.apply(fromBson(result.getFirst())));
  }

  private Optional<Path> getPath(final Request request) {
    return ofNullable(request.path)
        .map(path -> Path.parse(path, contextPath))
        .filter(path -> path.valid);
  }

  private Response getResults(
      final Request request,
      final List<BsonDocument> aggregation,
      final JsonObject jwt,
      final Path path) {
    return ok().withBody(
            with(toFlowPublisher(
                    getCollection(path)
                        .aggregate(
                            completeQuery(aggregation, jwt, noAcl(request)), BsonDocument.class)))
                .buffer(RESULT_SET_BUFFER)
                .map(BsonUtil::fromBson)
                .filter(json -> responseFilter.test(json, jwt))
                .map(addMyPrivileges(jwt))
                .get());
  }

  private CompletionStage<Response> handleRequest(
      final Request request, final JsonObject jwt, final Path path) {
    return switch (request.method) {
      case "DELETE" -> delete(jwt, path);
      case "GET" -> get(jwt, path);
      case "POST" -> post(request, jwt, path);
      case "PUT" -> put(request, jwt, path);
      default -> completedFuture(notImplemented());
    };
  }

  private boolean inspectStages(final JsonArray stages) {
    return stages(stages).noneMatch(this::isForbiddenStage);
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

  private boolean isForbiddenStage(final String stage) {
    return FORBIDDEN_STAGES.contains(stage) || (LOOKUP.equals(stage) && !warnLookup);
  }

  private static boolean isTextStage(final JsonValue stage) {
    return Optional.of(stage)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(o -> o.keySet().stream().filter(k -> k.equals(TEXT)).count() == 1)
        .orElse(false);
  }

  private JsonArray logWarnLookup(final JsonArray stages) {
    if (warnLookup && stages(stages).anyMatch(LOOKUP::equals)) {
      logger.warning(() -> "The $lookup stage was used in the aggregation " + stages);
    }

    return stages;
  }

  private void openDatabase() {
    if (mongoDatabase == null && mongoClient != null && mongoDatabaseName != null) {
      mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    }
  }

  private CompletionStage<Response> post(
      final Request request, final JsonObject jwt, final Path path) {
    return ofNullable(path.id)
        .map(
            id ->
                isCorrectObject(request, id)
                    ? sendCommand(
                        createObjectBuilder(request.body.asJsonObject())
                            .add(JWT, jwt)
                            .remove(MY_PRIVILEGES)
                            .build())
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
    return getPath(log(logger, request))
        .map(
            path ->
                getJwt(request)
                    .filter(jwt -> jwt.containsKey(SUB))
                    .map(jwt -> handleRequest(request, jwt, path).exceptionally(this::exception))
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
        .filter(this::inspectStages)
        .map(this::logWarnLookup)
        .map(stages -> getResults(request, fromJson(stages), jwt, path))
        .orElseGet(Response::badRequest);
  }

  private CompletionStage<Response> sendCommand(final JsonObject command) {
    final JsonObject c = completeCommand(command);

    return send(producer, new ProducerRecord<>(commandTopic(c), c.getString(ID), c))
        .thenApply(result -> must(result, r -> r))
        .thenApply(result -> accepted());
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

  public Server withWarnLookup(final boolean warnLookup) {
    this.warnLookup = warnLookup;

    return this;
  }

  private record Path(
      String app, String id, boolean sse, boolean sseSetup, String type, boolean valid) {
    private Path() {
      this(null, null, false, false, null, false);
    }

    private static Path parse(final String path, final String[] contextPath) {
      final String[] segments = getSegments(path, "/").toArray(String[]::new);

      return !hasPrefix(segments, contextPath)
          ? new Path()
          : withValue(copyOfRange(segments, contextPath.length, segments.length))
              .or(p -> p.length == 1, p -> new Path().withType(p[0]))
              .or(p -> p.length == 2 && !isUUID(p[1]), p -> new Path().withApp(p[0]).withType(p[1]))
              .or(p -> p.length == 2 && isUUID(p[1]), p -> new Path().withType(p[0]).withId(p[1]))
              .or(
                  p -> p.length == 3 && isUUID(p[2]),
                  p -> new Path().withApp(p[0]).withType(p[1]).withId(p[2]))
              .get()
              .map(Path.class::cast)
              .orElseGet(Path::new);
    }

    private String fullType() {
      return (app != null && !type.contains("-") ? (app + "-") : "") + (type != null ? type : "");
    }

    private Path withApp(final String app) {
      return new Path(app, id, sse, sseSetup, type, true);
    }

    private Path withId(final String id) {
      return new Path(app, id, sse, sseSetup, type, true);
    }

    private Path withType(final String type) {
      return new Path(app, id, sse, sseSetup, type, true);
    }
  }
}
