package org.hittepit.smapapi.transaction

import scala.annotation.tailrec
import java.sql.Connection
import org.hittepit.smapapi.core.Session

trait TransactionMode
object ReadOnly extends TransactionMode{
  override def toString() = "read only"
}
object Updatable extends TransactionMode{
  override def toString() = "read only"
}

object TransactionContext {
  def apply(sess:Session, ro: TransactionMode = ReadOnly) = new TransactionContext(None) with BaseTransactionContext { val session = sess; val readonly = ro == ReadOnly }
  def apply(parent: TransactionContext) = new TransactionContext(Some(parent))
}

class TransactionContext(val parent: Option[TransactionContext]) {
  def getSession():Session = {
    @tailrec
    def recursGetSession(t: TransactionContext): Session = t match {
      case b: BaseTransactionContext => b.session
      case _ => recursGetSession(t.parent.get)
    }

    recursGetSession(TransactionContext.this)
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
  val session:Session
  val readonly: Boolean
  var rollback = false
}