package org.hittepit.smapapi.transaction

import org.scalatest.WordSpec
import java.sql.Connection
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import javax.sql.DataSource
import org.scalatest.MustMatchers
import org.slf4j.LoggerFactory
import org.hittepit.smapapi.core.ReadOnlySession
import org.hittepit.smapapi.core.Session
import org.hittepit.smapapi.core.UpdatableSession

class TestTransactionManager extends WordSpec with MockitoSugar with MustMatchers{
  
	trait Transaction {
		val connection = mock[Connection]
		val ds = mock[DataSource]
		when(ds.getConnection()).thenReturn(connection)
		val tm = new TransactionManager(ds)
	}
	
	"The transaction manager" when {
	  "starting a new read-only transaction" must {
	    "create a new readOnly transaction without parent transaction" in new Transaction{
	      val t = tm.startNestedTransaction(ReadOnly)
	      t.isReadonly must be(true)
	      t.getSession.connection must be(connection)
	      t.parent must be(None)
	    }
	    "create a readOnlySession" in new Transaction{
	      val t = tm.startNestedTransaction(ReadOnly)
	      t.getSession mustBe a [ReadOnlySession]
	      t.getSession must not be a [UpdatableSession]
	      t.getSession.connection must be(connection)
	    }
	  }
	  
	  "starting a read-only transaction while a readonly transaction is already running" must {
	    "create a new readOnly transaction encapsulating the parent transaction" in new Transaction{
	      val t1 = tm.startNestedTransaction(ReadOnly)
	      t1.parent must be(None)
	      
	      val t2 = tm.startNestedTransaction(ReadOnly)
	      t2.isReadonly must be(true)
	      t2.getSession.connection must be(connection)
	      t2.parent must be(Some(t1))
	    }
	  }
	  
	  "starting an updatable transaction" must {
	    "create a new non readOnly transaction without parent transaction" in new Transaction{
	      val t = tm.startNestedTransaction(Updatable)
	      t.isReadonly must be(false)
	      t.parent must be(None)
	    }
	    "create a Session" in new Transaction{
	      val t = tm.startNestedTransaction(Updatable)
	      t.getSession mustBe a [UpdatableSession]
	      t.getSession.connection must be(connection)
	    }
	  }
	  
	  "starting an updatable transaction while another updatable transaction is running" must {
	    "create a new non readOnly transaction encapsultaing the parent transaction" in new Transaction{
	      val t1 = tm.startNestedTransaction(Updatable)
	      t1.parent must be(None)
	      
	      val t2 = tm.startNestedTransaction(Updatable)
	      t2.isReadonly must be(false)
	      t2.getSession.connection must be(connection)
	      t2.parent must be(Some(t1))
	    }
	  }
	  
	  "starting a new or nested transaction without specifying the transaction mode" must {
	    "create a read-only transaction by default" in new Transaction {
	      val t1 = tm.startNestedTransaction()
	      t1.isReadonly must be(true)
	      
	      val t2 = tm.startNestedTransaction()
	      t2.isReadonly must be(true)
	    }
	  }

	  "starting a readonly transaction while an updatable transaction is already running" must {
	    "create a new transaction that remains in updatable mode" in new Transaction{
	      val t1 = tm.startNestedTransaction(Updatable)
	      val t2 = tm.startNestedTransaction(ReadOnly)
	      t2.isReadonly must be(false)
	    }
	  }
	  
	  "starting an updatable transaction while a readonly transaction is already running" must {
	    "throw an exception" in new Transaction{
	      val t1 = tm.startNestedTransaction(ReadOnly)
	      an [Exception] must be thrownBy(tm.startNestedTransaction(Updatable))
	    }
	  }
	  
	  "tryning to close a transaction while no transaction is running" must {
	    "throw an exception" in new Transaction {
	      an  [RuntimeException] must be thrownBy(tm.closeNestedTransaction)
	    }
	  }
	}
}