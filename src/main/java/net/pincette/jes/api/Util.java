package net.pincette.jes.api;

import static java.lang.String.join;
import static java.util.stream.Collectors.joining;

import java.util.Map;

class Util {
  private Util() {}

  static String headersToString(final Map<String, String[]> headers) {
    return headers.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + join(",", entry.getValue()))
        .collect(joining("\n"));
  }
}
