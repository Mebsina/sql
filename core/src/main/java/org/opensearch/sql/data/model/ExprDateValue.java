/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import com.google.common.base.Objects;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.utils.DateTimeFormatters;

/** Expression Date Value. */
@RequiredArgsConstructor
public class ExprDateValue extends AbstractExprValue {

  private final LocalDate date;

  /**
   * Constructor with date string.
   *
   * @param date a date or timestamp string (does not accept time string)
   */
  public ExprDateValue(String date) {
    try {
      this.date = LocalDate.parse(date, DateTimeFormatters.DATE_TIMESTAMP_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new ExpressionEvaluationException(
          String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", date));
    }
  }

  @Override
  public String value() {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(date);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.DATE;
  }

  @Override
  public LocalDate dateValue() {
    return date;
  }

  @Override
  public LocalTime timeValue() {
    return LocalTime.of(0, 0, 0);
  }

  @Override
  public Instant timestampValue() {
    return ZonedDateTime.of(date, timeValue(), ZoneOffset.UTC).toInstant();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("DATE '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    return date.compareTo(other.dateValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return date.equals(other.dateValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(date);
  }
}
