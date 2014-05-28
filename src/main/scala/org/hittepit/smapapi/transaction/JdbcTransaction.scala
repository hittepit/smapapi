package org.hittepit.smapapi.transaction

import java.sql.Connection

import org.slf4j.LoggerFactory

trait JdbcTransaction extends TransactionManager{
  override val logger = LoggerFactory.getLogger(classOf[JdbcTransaction])

  def inTransaction[T](f: Connection => T): T = {
    var result: Option[T] = None
    val transaction = startNestedTransaction(Updatable)
    try {
      result = Some(f(transaction.getConnection))
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

  def readOnly[T](f: Connection => T): T = {
    var result: Option[T] = None
    val transaction = startNestedTransaction(ReadOnly)
    try {
      result = Some(f(transaction.getConnection))
    } catch {
      case e: Throwable =>
        logger.warn("Exception catched in execution. Transaction is in readonly mode.", e)
        throw e
    } finally {
      closeNestedTransaction
    }
    result.get
  }
}