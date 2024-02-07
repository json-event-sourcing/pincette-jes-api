module net.pincette.jes.api {
  requires net.pincette.common;
  requires java.json;
  requires org.mongodb.driver.core;
  requires org.mongodb.driver.reactivestreams;
  requires java.logging;
  requires net.pincette.jes;
  requires net.pincette.jes.util;
  requires net.pincette.json;
  requires net.pincette.jwt;
  requires net.pincette.mongo;
  requires net.pincette.rs;
  requires org.reactivestreams;
  requires com.auth0.jwt;
  requires net.pincette.jes.elastic;
  requires net.pincette.kafka.json;
  requires kafka.clients;
  requires org.mongodb.bson;

  exports net.pincette.jes.api;
}
