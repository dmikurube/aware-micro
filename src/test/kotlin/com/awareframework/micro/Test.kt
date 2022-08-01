package com.awareframework.micro

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(VertxExtension::class)
class Test {
  /*
  @BeforeEach
  fun prepare(vertx: Vertx, testContext: VertxTestContext) {
    vertx.deployVerticle(MainVerticle(), testContext.succeeding())  // void in Vert.x 3
  }
  */

  @Test
  fun testNothing(vertx: Vertx, testContext: VertxTestContext) {
    assertEquals(0, 0)
    /*
    testContext.succeeding({ buffer -> testContext.verify(() -> {
      testContext.completeNow();
    })})
    */
  }

  /*
  @Test
  fun testSimple(vertx: Vertx , testContext: VertxTestContext) {
  }
  */
}
