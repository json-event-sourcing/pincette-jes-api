module net.pincette.jes.api {
  requires org.mongodb.driver.core;
  requires org.mongodb.driver.reactivestreams;
  requires net.pincette.jes;
  requires net.pincette.jes.util;
  requires net.pincette.jwt;
  requires net.pincette.mongo;
  requires net.pincette.rs;
  requires org.reactivestreams;
  requires com.auth0.jwt;
  requires org.mongodb.bson;
  requires net.pincette.kafka.json;
  requires kafka.clients;
  requires net.pincette.json;
  requires net.pincette.common;
  requires java.json;
  requires java.logging;

  exports net.pincette.jes.api;
}
