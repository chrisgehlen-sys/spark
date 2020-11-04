/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

class DurationSuite extends TestSuiteBase {

  test("less") {
    assert(Duration(999) < Duration(1000))
    assert(Duration(0) < Duration(1))
    assert(!(Duration(1000) < Duration(999)))
    assert(!(Duration(1000) < Duration(1000)))
  }

  test("lessEq") {
    assert(Duration(999) <= Duration(1000))
    assert(Duration(0) <= Duration(1))
    assert(!(Duration(1000) <= Duration(999)))
    assert(Duration(1000) <= Duration(1000))
  }

  test("greater") {
    assert(!(Duration(999) > Duration(1000)))
    assert(!(Duration(0) > Duration(1)))
    assert(Duration(1000) > Duration(999))
    assert(!(Duration(1000) > Duration(1000)))
  }

  test("greaterEq") {
    assert(!(Duration(999) >= Duration(1000)))
    assert(!(Duration(0) >= Duration(1)))
    assert(Duration(1000) >= Duration(999))
    assert(Duration(1000) >= Duration(1000))
  }

  test("plus") {
    assert((Duration(1000) + Duration(100)) == Duration(1100))
    assert((Duration(1000) + Duration(0)) == Duration(1000))
  }

  test("minus") {
    assert((Duration(1000) - Duration(100)) == Duration(900))
    assert((Duration(1000) - Duration(0)) == Duration(1000))
    assert((Duration(1000) - Duration(1000)) == Duration(0))
  }

  test("times") {
    assert((Duration(100) * 2) == Duration(200))
    assert((Duration(100) * 1) == Duration(100))
    assert((Duration(100) * 0) == Duration(0))
  }

  test("div") {
    assert((Duration(1000) / Duration(5)) == 200.0)
    assert((Duration(1000) / Duration(1)) == 1000.0)
    assert((Duration(1000) / Duration(1000)) == 1.0)
    assert((Duration(1000) / Duration(2000)) == 0.5)
  }

  test("isMultipleOf") {
    assert(Duration(1000).isMultipleOf(Duration(5)))
    assert(Duration(1000).isMultipleOf(Duration(1000)))
    assert(Duration(1000).isMultipleOf(Duration(1)))
    assert(!Duration(1000).isMultipleOf(Duration(6)))
  }

  test("min") {
    assert(Duration(999).min(Duration(1000)) == Duration(999))
    assert(Duration(1000).min(Duration(999)) == Duration(999))
    assert(Duration(1000).min(Duration(1000)) == Duration(1000))
  }

  test("max") {
    assert(Duration(999).max(Duration(1000)) == Duration(1000))
    assert(Duration(1000).max(Duration(999)) == Duration(1000))
    assert(Duration(1000).max(Duration(1000)) == Duration(1000))
  }

  test("isZero") {
    assert(Duration(0).isZero)
    assert(!(Duration(1).isZero))
  }

  test("Milliseconds") {
    assert(Duration(100) == Milliseconds(100))
  }

  test("Seconds") {
    assert(Duration(30 * 1000) == Seconds(30))
  }

  test("Minutes") {
    assert(Duration(2 * 60 * 1000) == Minutes(2))
  }

}
