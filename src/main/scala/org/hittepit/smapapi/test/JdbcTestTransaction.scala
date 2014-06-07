package org.hittepit.smapapi.test

import org.hittepit.smapapi.transaction.JdbcTransaction
import java.sql.Connection
import org.hittepit.smapapi.transaction.Updatable
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.hittepit.smapapi.core.Session

trait JdbcTestTransaction extends JdbcTransaction{
  val logger:Logger
  
  def withRollback[T](f: Session => T): T = {
    var result: Option[T] = None
    val transaction = transactionManager.startNestedTransaction(Updatable)
    try {
      result = Some(f(transaction.getSession))
      transaction.setRollback
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
}