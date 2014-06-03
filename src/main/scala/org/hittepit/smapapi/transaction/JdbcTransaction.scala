package org.hittepit.smapapi.transaction

import java.sql.Connection
import org.slf4j.LoggerFactory
import org.slf4j.Logger

trait JdbcTransaction{
  val transactionManager:TransactionManager
  val logger:Logger

  def inTransaction[T](f: Connection => T): T = {
    var result: Option[T] = None
    val transaction = transactionManager.startNestedTransaction(Updatable)
    try {
      result = Some(f(transaction.getConnection))
    } catch {
      case e: Throwable =>
        logger.info("Exception catched in execution. Mark transaction for rollback.", e)
        transaction.setRollback
        throw e
    } finally {
      transactionManager.closeNestedTransaction
    }
    result.get
  }

  def readOnly[T](f: Connection => T): T = {
    var result: Option[T] = None
    val transaction = transactionManager.startNestedTransaction(ReadOnly)
    try {
      result = Some(f(transaction.getConnection))
    } catch {
      case e: Throwable =>
        logger.info("Exception catched in execution. Transaction is in readonly mode.", e)
        throw e
    } finally {
      transactionManager.closeNestedTransaction
    }
    result.get
  }
}