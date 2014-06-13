package org.hittepit.smapapi.transaction

import java.sql.Connection
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.hittepit.smapapi.core.Session

trait JdbcTransaction{
  val transactionManager:TransactionManager
  val logger:Logger

  def inTransaction[T](f: Session => T): T = {
    val transaction = transactionManager.startNestedTransaction(Updatable)
    try {
      f(transaction.getSession)
    } catch {
      case e: Throwable =>
        logger.info("Exception catched in execution. Mark transaction for rollback.", e)
        transaction.setRollback
        throw e
    } finally {
      transactionManager.closeNestedTransaction
    }
  }

  def readOnly[T](f: Session => T): T = {
    val transaction = transactionManager.startNestedTransaction(ReadOnly)
    try {
      f(transaction.getSession)
    } catch {
      case e: Throwable =>
        logger.info("Exception catched in execution. Transaction is in readonly mode.", e)
        transaction.setRollback //TODO vérifier, readonly sur 
        throw e
    } finally {
      transactionManager.closeNestedTransaction
    }
  }
}