package org.hittepit.smapapi.test

import org.hittepit.smapapi.transaction.JdbcTransaction
import java.sql.Connection
import org.hittepit.smapapi.transaction.Updatable
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.hittepit.smapapi.core.session.Session

trait JdbcTestTransaction extends JdbcTransaction{
  val logger:Logger
  
  def withRollback[T](f: Session => T): T = {
    val transaction = transactionManager.startNestedTransaction(Updatable)
    try {
      f(transaction.getSession.asInstanceOf[Session])
    } catch {
      case e: Throwable =>
        logger.info("Exception catched in execution. Mark transaction for rollback.", e)
        throw e
    } finally {
      transaction.setRollback
      transactionManager.closeNestedTransaction
    }
  }
}