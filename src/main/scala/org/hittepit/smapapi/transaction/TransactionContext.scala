package org.hittepit.smapapi.transaction

import scala.annotation.tailrec
import java.sql.Connection

class ReadOnly
object ReadOnly {
  def apply() = new ReadOnly
}

object TransactionContext {
  def apply(con: Connection, ro: ReadOnly = null) = new TransactionContext(None) with BaseTransactionContext { val connection = con; val readonly = ro != null }
  def apply(parent: TransactionContext) = new TransactionContext(Some(parent))
}

class TransactionContext(val parent: Option[TransactionContext]) {
  def getConnection(): Connection = {
    @tailrec
    def recursGetConnection(t: TransactionContext): Connection = t match {
      case b: BaseTransactionContext => b.connection
      case _ => recursGetConnection(t.parent.get)
    }

    recursGetConnection(TransactionContext.this)
  }

  def setRollback: Unit = {
    @tailrec
    def recSetRollback(t: TransactionContext): Unit = t match {
      case bt: BaseTransactionContext => bt.rollback = true
      case _ => recSetRollback(t.parent.get)
    }

    recSetRollback(TransactionContext.this)
  }

  def isRollback: Boolean = {
    @tailrec
    def recIsRollback(t: TransactionContext): Boolean = t match {
      case bt: BaseTransactionContext => bt.rollback
      case _ => recIsRollback(t.parent.get)
    }

    recIsRollback(TransactionContext.this)
  }

  def isReadonly: Boolean = {
    @tailrec
    def recIsReadonly(t: TransactionContext): Boolean = t match {
      case bt: BaseTransactionContext => bt.readonly
      case _ => recIsReadonly(t.parent.get)
    }

    recIsReadonly(TransactionContext.this)
  }
}

trait BaseTransactionContext {
  val connection: Connection
  val readonly: Boolean
  var rollback = false
}