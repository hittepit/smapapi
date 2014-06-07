package org.hittepit.smapapi.transaction

import org.slf4j.LoggerFactory
import javax.sql.DataSource
import org.slf4j.Logger
import org.hittepit.smapapi.core.Session

class TransactionManager(ds:DataSource) {
  val dataSource:DataSource = ds
  val logger:Logger = LoggerFactory.getLogger(classOf[TransactionManager])

  private val transactionThreadLocal = new ThreadLocal[Option[TransactionContext]]
  transactionThreadLocal.set(None)
 

  def startNestedTransaction(ro: TransactionMode = ReadOnly): TransactionContext = {
    def createOrDecoreTransaction(parent: Option[TransactionContext], readOnly: TransactionMode) = parent match {
      case None =>
          logger.debug("Creating new base transaction, readonly = {}", readOnly)
		  val connection = dataSource.getConnection()
		  connection.setAutoCommit(false)
		  val session = new Session(connection)
		  TransactionContext(session, readOnly)
      case Some(parentTransaction) =>
       	logger.debug("Creating new surrounding transaction, base transaction is readOnly = {}",parentTransaction.isReadonly)
        if(logger.isDebugEnabled()){
        	logger.debug("Base transaction is readOnly = {}",parentTransaction.isReadonly)
        }
      	TransactionContext(parentTransaction)
    }

    val transaction = createOrDecoreTransaction(transactionThreadLocal.get(), ro)

    transactionThreadLocal.set(Some(transaction))
    transaction
  }

  def closeNestedTransaction: Option[TransactionContext] = {
    transactionThreadLocal.get match {
      case None => throw new RuntimeException("Attempt to close a non-existing transaction.")
      case Some(transaction) =>
		    transaction.parent match {
		      case Some(t) =>
		        logger.debug("Closing surrounding transaction")
		        transactionThreadLocal.set(Some(t))
		        Some(t)
		      case None =>
		        logger.debug("Closing base transaction")
		        transactionThreadLocal.set(None)
		        try {
		          if (!transaction.isReadonly) {
		            if (transaction.isRollback) {
		              logger.info("Rollback transaction")
		              transaction.getSession.rollback()
		            } else {
		              logger.info("Commiting transaction")
		              transaction.getSession.commit()
		            }
		          } else {
		            logger.info("Readonly transaction, no commit, not rollback")
		          }
		          None
		        } catch {
		          case e: Throwable =>
		            logger.error("Exception caught while closing the transaction", e)
		            throw e
		        } finally {
		          try {
		            logger.info("Closing connection...")
		            transaction.getSession.close()
		            logger.info("Connection closed.")
		          } catch {
		            case e: Throwable =>
		              logger.error("Excepton caught while closing the connection", e)
		              throw e
		          }
		        }
		    }
    }
  }
}