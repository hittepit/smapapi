package org.hittepit.smapapi.transaction

import org.slf4j.LoggerFactory

import javax.sql.DataSource

trait TransactionManager {
  val dataSource:DataSource
  val logger = LoggerFactory.getLogger(classOf[TransactionManager])

  private val transactionThreadLocal = new ThreadLocal[Option[TransactionContext]]
  transactionThreadLocal.set(None)
 

  def startNestedTransaction(ro: TransactionMode = ReadOnly): TransactionContext = {
    def createOrDecoreTransaction(parent: Option[TransactionContext], readOnly: TransactionMode) = parent match {
      case None =>
          logger.debug("Creating new base transaction, readonly = {}", readOnly)
		  val connection = dataSource.getConnection()
		  connection.setAutoCommit(false)
		  TransactionContext(connection, readOnly)
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
		              transaction.getConnection.rollback()
		            } else {
		              logger.info("Commiting transaction")
		              transaction.getConnection.commit()
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
		            transaction.getConnection.close()
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