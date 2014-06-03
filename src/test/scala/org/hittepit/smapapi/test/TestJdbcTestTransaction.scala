package org.hittepit.smapapi.test

import org.scalatest.WordSpec

class TestJdbcTestTransaction extends WordSpec {
  "inTransactionTest" when {
    "invoked on a function that succeed" must {
      "mark the transaction for rollback" in {
        pending
      }
      "return the result of the function" in {
        pending
      }
    }
    "invoked on a function that throws an exception" must {
      "mark the transaction for rollback" in {
        pending
      }
      "rethrow the exception" in {
        pending
      }
    }
  }
}