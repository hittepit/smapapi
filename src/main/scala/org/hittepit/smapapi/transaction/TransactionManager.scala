package org.hittepit.smapapi.transaction

import org.slf4j.LoggerFactory

import javax.sql.DataSource

trait TransactionManager {
  val dataSource:DataSource
  val logger = LoggerFactory.getLogger(classOf[TransactionManager])

  val transactionThreadLocal = new ThreadLocal[TransactionContext]

  def startNestedTransaction(ro: ReadOnly = null): TransactionContext = {
    def createOrDecoreTransaction(parent: TransactionContext, readOnly: ReadOnly) = if (parent == null) {
      val connection = dataSource.getConnection()
      connection.setAutoCommit(false)
      TransactionContext(connection, readOnly)
    } else {
      TransactionContext(parent)
    }

    val transaction = createOrDecoreTransaction(transactionThreadLocal.get(), ro)

    transactionThreadLocal.set(transaction)
    transaction
  }

  def closeNestedTransaction: Option[TransactionContext] = {
    var transaction = transactionThreadLocal.get
    if (transaction == null) {
      throw new RuntimeException("Transaction was closed while there was no transaction started.")
    }
    transaction.parent match {
      case Some(t) =>
        transactionThreadLocal.set(t)
        Some(t)
      case None =>
        transactionThreadLocal.remove()
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