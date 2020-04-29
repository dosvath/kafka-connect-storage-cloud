/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.format;

import io.confluent.connect.s3.format.RecordViews.ValueRecordView;

import static java.util.Objects.requireNonNull;

public class RecordViewSetter {
  protected RecordView recordView = new ValueRecordView();

  public void setRecordView(RecordView recordView) {
    this.recordView = requireNonNull(recordView);
  }

  protected String getAdjustedFilename(String filename, String extension) {
    int extensionOffset = filename.indexOf(extension);
    return extensionOffset > -1
        ? filename.substring(0, extensionOffset) + recordView.getExtension()
        + filename.substring(extensionOffset)
        : filename;
  }
}
