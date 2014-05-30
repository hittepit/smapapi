package org.hittepit.smapapi.test

import org.hittepit.smapapi.transaction.JdbcTransaction
import java.sql.Connection
import org.hittepit.smapapi.transaction.Updatable

trait JdbcTestTransaction extends JdbcTransaction{
  def inTestTransaction[T](f: Connection => T): T = {
    var result: Option[T] = None
    val transaction = startNestedTransaction(Updatable)
    try {
      result = Some(f(transaction.getConnection))
      transaction.setRollback
    } catch {
      case e: Throwable =>
        logger.warn("Exception catched in execution. Mark transaction for rollback.", e)
        transaction.setRollback
        throw e
    } finally {
      closeNestedTransaction
    }
    result.get
  }
}