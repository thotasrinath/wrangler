/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A Wrangler plugin that applies shuffling masking on the column.
 *
 * <p>
 *   Fixed length shuffle masking performs obfuscation by using random character
 *   substitution method. The data is randomly shuffled in the column.
 *
 *   <blockquote>
 *     <pre>
 *       Executor step = new MaskNumber(lineno, line, "150 Mars Avenue, Marcity, Mares", 2);
 *     </pre>
 *   </blockquote>
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(MaskShuffle.NAME)
@Categories(categories = { "transform"})
@Description("Masks a column value by shuffling characters while maintaining the same length.")
public class MaskShuffle implements Directive {
  public static final String NAME = "mask-shuffle";
  // Column on which to apply mask.
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      Row masked = new Row(row);
      int idx = row.find(column);
      if (idx != -1) {
        masked.setValue(idx, maskShuffle((String) row.getValue(idx), 0));
      } else {
        throw new DirectiveExecutionException(toString() + " : '" +
                                  column + "' column is not defined. Please check the wrangling step."
        );
      }
      results.add(masked);
    }

    return results;
  }

  private String maskShuffle(String str, int seed) {
    final String cons = "bcdfghjklmnpqrstvwxz";
    final String vowel = "aeiouy";
    final String digit = "0123456789";

    Random r = new Random(seed);
    char data[] = str.toCharArray();

    for (int n = 0; n < data.length; ++n) {
      char ln = Character.toLowerCase(data[n]);
      if (cons.indexOf(ln) >= 0) {
        data[n] = randomChar(r, cons, ln != data[n]);
      } else if (vowel.indexOf(ln) >= 0) {
        data[n] = randomChar(r, vowel, ln != data[n]);
      } else if (digit.indexOf(ln) >= 0) {
        data[n] = randomChar(r, digit, ln != data[n]);
      }
    }
    return new String(data);
  }

  private char randomChar(Random r, String cs, boolean uppercase) {
    char c = cs.charAt(r.nextInt(cs.length()));
    return uppercase ? Character.toUpperCase(c) : c;
  }
}

