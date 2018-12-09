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

package co.cask.wrangler.config;

import co.cask.wrangler.api.DirectiveConfig;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DirectiveConfig}
 *
 * @see DirectiveConfig
 * @see co.cask.wrangler.api.DirectiveAlias
 * @see co.cask.wrangler.api.DirectiveEnforcer
 */
public class DirectiveConfigTest {

  private static final String SPECIFICATION = "{\n" +
    "\t\"exclusions\" : [\n" +
    "\t\t\"parse-as-csv\",\n" +
    "\t\t\"parse-as-excel\",\n" +
    "\t\t\"set\",\n" +
    "\t\t\"invoke-http\"\n" +
    "\t],\n" +
    "\n" +
    "\t\"aliases\" : {\n" +
    "\t\t\"json-parser\" : \"parse-as-json\",\n" +
    "\t\t\"js-parser\" : \"parse-as-json\"\n" +
    "\t}\n" +
    "}";

  private static final String ONLY_EXCLUSIONS = "\n" +
    "{\n" +
    "\t\"exclusions\" : [\n" +
    "\t\t\"parse-as-csv\",\n" +
    "\t\t\"parse-as-excel\",\n" +
    "\t\t\"set\",\n" +
    "\t\t\"invoke-http\"\n" +
    "\t]\n" +
    "}";

  private static final String ONLY_ALIASES = "{\n" +
    "\t\"aliases\" : {\n" +
    "\t\t\"json-parser\" : \"parse-as-json\",\n" +
    "\t\t\"js-parser\" : \"parse-as-json\"\n" +
    "\t}\n" +
    "}";

  private static final String EMPTY = "{}";

  @Test
  public void testParsingOfConfiguration() throws Exception {
    DirectiveConfig config = new Gson().fromJson(SPECIFICATION, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(true, config.isExcluded("parse-as-csv"));
    Assert.assertEquals(false, config.isExcluded("parse-as-json"));
    Assert.assertEquals("parse-as-json", config.getAliasName("json-parser"));
  }

  @Test
  public void testParsingOnlyExclusions() throws Exception {
    DirectiveConfig config = new Gson().fromJson(ONLY_EXCLUSIONS, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(true, config.isExcluded("parse-as-csv"));
    Assert.assertEquals(false, config.isExcluded("parse-as-json"));
    Assert.assertEquals(null, config.getAliasName("json-parser"));
  }

  @Test
  public void testParsingOnlyAliases() throws Exception {
    DirectiveConfig config = new Gson().fromJson(ONLY_ALIASES, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(false, config.isExcluded("parse-as-csv"));
    Assert.assertEquals("parse-as-json", config.getAliasName("json-parser"));
  }

  @Test
  public void testParsingEmpty() throws Exception {
    DirectiveConfig config = new Gson().fromJson(EMPTY, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(false, config.isExcluded("parse-as-csv"));
    Assert.assertEquals(null, config.getAliasName("json-parser"));
  }
}
