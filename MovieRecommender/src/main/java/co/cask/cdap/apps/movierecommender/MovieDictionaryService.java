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

package co.cask.cdap.apps.movierecommender;

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;

/**
 * A {@link Service} that provides access to movies dictionary.
 */
public class MovieDictionaryService extends AbstractService {
  @Override
  protected void configure() {
    setName("MovieDictionaryService");
    setDescription("Service to store moviesStore information to dataset");
    useDataset("movies");
    addHandler(new MovieDictionaryServiceHandler());
  }
}
