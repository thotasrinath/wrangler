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

package co.cask.wrangler.registry;

import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveRegistry;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * This class implements a Composition of multiple registries.
 *
 * <p>It implements the <tt>DirectiveRegistry</tt> interface. Search for a directives
 * uses the order of the directives specified at the construction time.</p>
 *
 * <p>If the directive is not found in any of the registry, then a <tt>null</tt>
 * is returned.</p>
 */
public final class CompositeDirectiveRegistry implements DirectiveRegistry {
  private final DirectiveRegistry[] registries;

  public CompositeDirectiveRegistry(DirectiveRegistry ... registries) {
    this.registries = registries;
  }

  /**
   * This method looks for the <tt>directive</tt> in all the registered registries.
   *
   * <p>The order of search is as specified by the collection order. Upon finding
   * the first valid instance of directive, the <tt>DirectiveInfo</tt> is returned.</p>
   *
   * @param directive of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Nullable
  @Override
  public DirectiveInfo get(String directive) throws DirectiveLoadException {
    for (int i = 0; i < registries.length; ++i) {
      DirectiveInfo info = registries[i].get(directive);
      if (info != null) {
        return info;
      }
    }
    return null;
  }

  @Override
  public void reload() throws DirectiveLoadException {
    for (int idx = 0; idx < registries.length; ++idx) {
      registries[idx].reload();
    }
  }

  /**
   * Returns an <tt>JsonElement</tt> representation of this implementation of object.
   * Arrays, Sets are represented as <tt>JsonArray</tt> and other object and map types
   * are represented as <tt>JsonObject</tt>.
   *
   * @return An instance of {@link JsonElement} of this object.
   */
  @Override
  public JsonElement toJson() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < registries.length; ++i) {
      JsonElement element = registries[i].toJson();
      array.add(element);
    }
    return array;
  }

  /**
   * @return Returns an iterator to iterate through all the <code>DirectiveInfo</code> objects
   * maintained within the registry.
   */
  @Override
  public Iterator<DirectiveInfo> iterator() {
    return new CompositeRegistryIterator();
  }

  /**
   * Closes any resources acquired during initialization or otherwise.
   */
  @Override
  public void close() throws IOException {
    for (int idx = 0; idx < registries.length; ++idx) {
      registries[idx].close();
    }
  }

  /**
   * This class <code>CompositeRegistryIterator</code> iterators each of the
   * registeries in entirity before moving on to the next registry managed
   * by the <code>CompositeRegistryIterator</code> class.
   */
  final class CompositeRegistryIterator implements Iterator<DirectiveInfo> {
    private final int count;
    private int idx;
    private Iterator<DirectiveInfo> iterator;

    CompositeRegistryIterator() {
      this.count = registries.length;
      this.idx = 0;
      this.iterator = registries[idx].iterator();
    }

    /**
     * @return Returns true if there exists registry that's not been iterated in entiry, else false.
     */
    @Override
    public boolean hasNext() {
      boolean status = iterator.hasNext();
      if (status == false && idx < count - 1) {
        idx = idx + 1;
        iterator = registries[idx].iterator();
        if (iterator.hasNext()) {
          status = true;
        }
      }
      return status;
    }

    /**
     * @return The next <code>DirectiveInfo</code> from the registry.
     */
    @Override
    public DirectiveInfo next() {
      return iterator.next();
    }

    @Override
    public void remove() {
      // no-op
    }
  }
}
