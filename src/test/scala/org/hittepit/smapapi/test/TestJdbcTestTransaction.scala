package org.hittepit.smapapi.test

import org.scalatest.WordSpec
import org.hittepit.smapapi.transaction.TransactionManager
import java.sql.Connection
import javax.sql.DataSource
import org.slf4j.LoggerFactory
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.MustMatchers

class TestJdbcTestTransaction extends WordSpec with MustMatchers with MockitoSugar {
  trait TestTransactionalEnvironment{
    val connection = mock[Connection]
    val ds = mock[DataSource]
	when(ds.getConnection()).thenReturn(connection)
    val jdbcTransaction = new JdbcTestTransaction{
      val transactionManager=new TransactionManager(ds)
    }
  }
  
  "withRollback" when {
    "invoked on a function that succeed" must {
      "mark the transaction for rollback" in new TestTransactionalEnvironment{
        jdbcTransaction.withRollback{con => 7}
        verify(connection, times(1)).rollback()
      }
      "return the result of the function" in new TestTransactionalEnvironment{
        val r = jdbcTransaction.withRollback{con => 7}
        r must be(7)
      }
    }
    "invoked on a function that throws an exception" must {
      "mark the transaction for rollback" in new TestTransactionalEnvironment{
        try{
        	jdbcTransaction.withRollback{con => throw new Exception}
        }catch{
          case _:Throwable =>
        }
        verify(connection, times(1)).rollback()
      }
      "rethrow the exception" in new TestTransactionalEnvironment{
        an[Exception] must be thrownBy(jdbcTransaction.withRollback{con => throw new Exception})
      }
    }
  }
}