package org.hittepit.smapapi.transaction

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.sql.Connection
import javax.sql.DataSource
import org.apache.tools.ant.taskdefs.JDBCTask
import org.scalatest.MustMatchers

class TestJdbcTransaction extends WordSpec with MockitoSugar with MustMatchers{
  trait TransactionalEnvironment{
    val connection = mock[Connection]
    val ds = mock[DataSource]
	when(ds.getConnection()).thenReturn(connection)
    val jdbcTransaction = new JdbcTransaction{val dataSource = ds}
  }
  
	"The JdbcTransaction Manager" when {
	  "invoking a standard method in simple transactional level" must {
	    "close the base transaction" in new TransactionalEnvironment{
	    }
	    "commit the transaction" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction(_=>1)
	      verify(connection, times(1)).commit()
	    }
	    "close the connection" in new TransactionalEnvironment {
	      val ret = jdbcTransaction.inTransaction(_=>1)
	      verify(connection, times(1)).close()
	    }
	    "return the value of the method" in new TransactionalEnvironment {
	      val ret = jdbcTransaction.inTransaction(_=>1)
	      ret must be(1)
	    }
	  }
	  
	  "invoking a standard method in nested transactional level" must {
	    "close the surrounding transaction" in new TransactionalEnvironment{
	      
	    }
	    "not commit the transaction" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction{con => 
	        val r = jdbcTransaction.inTransaction(_=>1)
	        verify(connection,never()).commit()
	      }
	    }
	    "not close the connection" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction{con => 
	        val r = jdbcTransaction.inTransaction(_=>1)
	        verify(connection,never()).close()
	      }
	    }
	    "return the value of the method" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction{con => 
	        val r = jdbcTransaction.inTransaction(_=>1)
	        r must be(1)
	      }
	    }
	    "not close the base transaction" in {
	      
	    }
	  }
	}
}