/*
 *  Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Executor to parse timestamp as date.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-timestamp")
@Categories(categories = {"parser", "date"})
@Description("Parses column values representing unix timestamp as date.")
public class ParseTimestamp implements Directive {
  public static final String NAME = "parse-timestamp";
  private static final Set<TimeUnit> SUPPORTED_TIME_UNITS = EnumSet.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS,
                                                                       TimeUnit.MICROSECONDS);
  private String column;
  private TimeUnit timeUnit;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("timeunit", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.timeUnit = TimeUnit.MILLISECONDS;

    if (args.contains("timeunit")) {
      String unitValue = ((Text) args.value("timeunit")).value();
      this.timeUnit = getTimeUnit(unitValue);
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        // If the data in the cell is null or is already of date format, then
        // continue to next row.
        if (object == null || object instanceof ZonedDateTime) {
          continue;
        }

        long longValue = getLongValue(object);
        ZonedDateTime zonedDateTime = getZonedDateTime(longValue, timeUnit, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        row.setValue(idx, zonedDateTime);
      }
    }
    return rows;
  }

  private static TimeUnit getTimeUnit(String unitValue) throws DirectiveParseException {
    TimeUnit unit;

    try {
      unit = TimeUnit.valueOf(unitValue.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new DirectiveParseException(String.format("'%s' is not a supported time unit. Supported time " +
                                                        "units are %s", unitValue, SUPPORTED_TIME_UNITS));
    }

    if (!SUPPORTED_TIME_UNITS.contains(unit)) {
      throw new DirectiveParseException(String.format("'%s' is not a supported time unit. Supported time " +
                                                        "units are %s", unitValue, SUPPORTED_TIME_UNITS));
    }

    return unit;
  }

  private long getLongValue(Object object) throws ErrorRowException {
    String errorMsg = String.format("%s : Invalid type '%s' of column '%s'. Must be of type Long or String.",
                                    toString(), object.getClass().getName(), column);
    try {
      if (object instanceof Long) {
        return  (long) object;
      } else if (object instanceof String) {
        return Long.parseLong((String) object);
      }
    } catch (Exception e) {
      // Exception while casting the object, do not handle it here, so that ErrorRowException is thrown.
      errorMsg = String.format("%s : Invalid value for column '%s'. Must be of type Long or String representing long.",
                               toString(), column);
    }

    throw new ErrorRowException(errorMsg, 2);
  }

  private ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }
}
