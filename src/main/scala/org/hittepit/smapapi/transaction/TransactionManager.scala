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
		  val connection = dataSource.getConnection()
		  connection.setAutoCommit(false)
		  TransactionContext(connection, readOnly)
      case Some(parentTransaction) =>
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
		        transactionThreadLocal.set(Some(t))
		        Some(t)
		      case None =>
		        transactionThreadLocal.set(None)
		        try {
		          if (!transaction.isReadonly) {
		            if (transaction.isRollback) {
		              transaction.getConnection.rollback()
		            } else {
		              transaction.getConnection.commit()
		            }
		          }
		          None
		        } catch {
		          case e: Throwable =>
		            logger.error("Exception caught whil closing the transaction", e)
		            throw e
		        } finally {
		          try {
		            transaction.getConnection.close()
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