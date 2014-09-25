/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.apps.wise;

import com.google.common.base.Objects;

/**
 * Defines the number of visits a web page has had, as well as the number of times
 * it was the last page visited in a short period of time.
 */
public class PageBounce {
  private final String uri;
  private final long totalVisits;
  private final long bounces;

  public PageBounce(String uri, long totalVisits, long bounces) {
    this.uri = uri;
    this.totalVisits = totalVisits;
    this.bounces = bounces;
  }

  public long getTotalVisits() {
    return totalVisits;
  }

  public long getBounces() {
    return bounces;
  }

  public String getUri() {
    return uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PageBounce that = (PageBounce) o;

    return Objects.equal(this.uri, that.uri) &&
      Objects.equal(this.totalVisits, that.totalVisits) &&
      Objects.equal(this.bounces, that.bounces);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uri, totalVisits, bounces);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("uri", uri)
      .add("totalVisits", totalVisits)
      .add("bounces", bounces)
      .toString();
  }
}
