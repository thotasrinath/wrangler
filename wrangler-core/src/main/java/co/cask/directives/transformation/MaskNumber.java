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
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.utils.TypeConvertor;

import java.util.List;

/**
 * A directive that applies substitution masking on the column.
 *
 * <p>
 *  Substitution masking is generally used for masking credit card or SSN numbers.
 *  This type of masking is fixed masking, where the pattern is applied on the
 *  fixed length string.
 *
 *  <ul>
 *    <li>Use of # will include the digit from the position.</li>
 *    <li>Use x/X to mask the digit at that position (converted to lowercase x in the result)</li>
 *    <li>Any other characters will be inserted as-is.</li>
 *  </ul>
 *
 *  <blockquote>
 *    <pre>
 *        Executor step = new MaskNumber(lineno, line, "ssn", "XXX-XX-####", 1);
 *        Executor step = new MaskNumber(lineno, line, "amex", "XXXX-XXXXXX-X####", 1);
 *    </pre>
 *  </blockquote>
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(MaskNumber.NAME)
@Categories(categories = { "transform"})
@Description("Masks a column value using the specified masking pattern.")
public class MaskNumber implements Directive {
  public static final String NAME = "mask-number";
  // Specifies types of mask
  public static final int MASK_NUMBER = 1;
  public static final int MASK_SHUFFLE = 2;

  // Masking pattern
  private String mask;

  // Column on which to apply mask.
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("mask", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.mask = ((Text) args.value("mask")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        String value = TypeConvertor.toString(row.getValue(idx));
        if (value == null) {
          continue;
        }
        row.setValue(idx, maskNumber(value, mask));
      } else {
        row.add(column, new String(""));
      }
    }
    return rows;
  }

  private String maskNumber(String number, String mask) {
    int index = 0;
    StringBuilder masked = new StringBuilder();
    for (int i = 0; i < mask.length(); i++) {
      char c = mask.charAt(i);
      if (c == '#') {
        // if we have print numbers and the mask index has exceed, we continue further.
        if (index > number.length() - 1) {
          continue;
        }
        masked.append(number.charAt(index));
        index++;
      } else if (c == 'x' || c == 'X') {
        masked.append(Character.toLowerCase(c));
        index++;
      } else {
        if (index < number.length()) {
          char c1 = number.charAt(index);
          if (c1 == c) {
            index++;
          }
        }
        masked.append(c);
      }
    }
    return masked.toString();
  }
}


