package com.oracle.fdi.spark.sql.fditcf.source;

import java.util.Map;

/**
 * Centralized parsing and validation for fditcf options.
 * Currently handles:
 *  - startingScn (Long)
 *  - endingScn (Long)
 * Enforces: if both provided, startingScn <= endingScn.
 */
public final class FdiTcfOptions {

  private final Long startingScn;
  private final Long endingScn;

  private FdiTcfOptions(Long startingScn, Long endingScn) {
    this.startingScn = startingScn;
    this.endingScn = endingScn;
  }

  public static FdiTcfOptions parse(Map<String, String> properties) {
    if (properties == null) {
      return new FdiTcfOptions(null, null);
    }
    String startStr = properties.get("startingScn");
    String endStr = properties.get("endingScn");
    Long start = parseLongOption("startingScn", startStr);
    Long end = parseLongOption("endingScn", endStr);
    validateRange(start, end);
    return new FdiTcfOptions(start, end);
  }

  private static Long parseLongOption(String name, String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
              "Option '" + name + "' must be a valid long but was: " + value, e);
    }
  }

  private static void validateRange(Long start, Long end) {
    if (start != null && end != null && start > end) {
      throw new IllegalArgumentException(
              "Invalid SCN range: startingScn (" + start + ") > endingScn (" + end + ")");
    }
  }

  public boolean hasStartingScn() {
    return startingScn != null;
  }

  public boolean hasEndingScn() {
    return endingScn != null;
  }

  public Long getStartingScn() {
    return startingScn;
  }

  public Long getEndingScn() {
    return endingScn;
  }

  @Override
  public String toString() {
    return "FdiTcfOptions{startingScn=" + startingScn + ", endingScn=" + endingScn + '}';
  }
}
