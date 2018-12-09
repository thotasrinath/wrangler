/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.directives.row;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SendToErrorAndContinue}
 */
public class SendToErrorAndContinueTest {

  @Test
  public void testErrorConditionTrueAndContinue() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error-and-continue exp:{C == 1}",
      "send-to-error-and-continue exp:{C == 2}",
      "send-to-error-and-continue exp:{D == 3.0}",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,C,D"),
      new Row("body", "X,Y,1,2.0"),
      new Row("body", "U,V,2,3.0")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testErrorConditionTrueAndContinueWithTransientVariable() throws Exception {
    String[] directives = new String[] {
            "parse-as-csv body , true",
            "drop body",
            "send-to-error-and-continue exp:{body_3 == 'xx'} 'invalid value'",
            "send-to-error-and-continue exp:{body_4=='1'} 'junk' ",
            "send-to-error exp:{dq_failure >= 1} "
    };

    List<Row> rows = Arrays.asList(
            new Row("body", "1020134.298,,1,2,2 "),
            new Row("body", "1020134.298,,xx,1,3"),
            new Row("body", "1020134.298,,4,1,4"),
            new Row("body", "1020134.298,,4,2,5"),
            new Row("body", "1020134.298,,1,2,1")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();
    Assert.assertEquals(2, errors.size());
    Assert.assertEquals(3, results.size());
  }
}
