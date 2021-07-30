/*
 * Copyright 2021 The DAPHNE Consortium
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <runtime/local/io/utils.h>

#include <limits>
#include <stdexcept>

void convert(std::string const &x, double *v) {
  try {
    *v = stod(x);
  } catch (const std::invalid_argument &) {
    *v = std::numeric_limits<double>::quiet_NaN();
  }
}
void convert(std::string const &x, float *v) {
  try {
    *v = stof(x);
  } catch (const std::invalid_argument &) {
    *v = std::numeric_limits<float>::quiet_NaN();
  }
}
void convert(std::string const &x, int8_t *v) { *v = stoi(x); }
void convert(std::string const &x, int32_t *v) { *v = stoi(x); }
void convert(std::string const &x, int64_t *v) { *v = stoi(x); }
void convert(std::string const &x, uint8_t *v) { *v = stoi(x); }
void convert(std::string const &x, uint32_t *v) { *v = stoi(x); }
void convert(std::string const &x, uint64_t *v) { *v = stoi(x); }