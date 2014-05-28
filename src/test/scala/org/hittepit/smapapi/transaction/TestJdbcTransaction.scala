package org.hittepit.smapapi.transaction

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.sql.Connection
import javax.sql.DataSource
import org.apache.tools.ant.taskdefs.JDBCTask
import org.scalatest.MustMatchers
import sun.reflect.generics.reflectiveObjects.NotImplementedException

class TestJdbcTransaction extends WordSpec with MockitoSugar with MustMatchers{
  trait TransactionalEnvironment{
    val connection = mock[Connection]
    val ds = mock[DataSource]
	when(ds.getConnection()).thenReturn(connection)
    val jdbcTransaction = new JdbcTransaction{val dataSource = ds}
  }
  
	"The JdbcTransaction Manager" when {
	  "invoking a standard method in simple transactional level" must {
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
	    "not commit the inner transaction" in new TransactionalEnvironment{
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
	  }
	  
  	  "invoking a method that throws an exception in simple transactional level" must {
  	    "forward the exception" in new TransactionalEnvironment{
  	      an [NotImplementedException] must be thrownBy{jdbcTransaction.inTransaction(_=>throw new NotImplementedException)}
  	    }
	    "rollback, not commit, the transaction" in new TransactionalEnvironment{
	      try{
	        jdbcTransaction.inTransaction(_=>throw new Exception)
	        fail
	      }catch{
	        case _:Exception =>
	        	verify(connection, times(1)).rollback()
	        	verify(connection, never).commit()
	      }
	    }
	    "close the connection" in new TransactionalEnvironment {
	      try{
	        jdbcTransaction.inTransaction(_=>throw new Exception)
	        fail
	      }catch{
	        case _:Exception =>
	        	verify(connection, times(1)).close()
	      }
	    }
	  }
	  
	  "invoking a method that throws an exception in nested transactional level" must {
  	    "forward the exception" in new TransactionalEnvironment{
  	      jdbcTransaction.inTransaction{con =>
  	        an [NotImplementedException] must be thrownBy{jdbcTransaction.inTransaction(_=>throw new NotImplementedException)}
  	      }
  	    }
	    "not commit nor rollback the inner transaction" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction{con => 
	        try{
	          jdbcTransaction.inTransaction(_=>throw new NotImplementedException)
	          fail
	        }catch{
	          case _:NotImplementedException =>
		        verify(connection,never()).commit()
		        verify(connection,never).rollback
	        }
	      }
	    }
	    "rollback, not commit, the outer transaction" in new TransactionalEnvironment{
	      try{
		      jdbcTransaction.inTransaction{con => 
		        jdbcTransaction.inTransaction(_=>throw new NotImplementedException)
		      }
		      fail
	      }catch{
	        case _:Exception =>
		        verify(connection,never()).commit()
		        verify(connection,times(1)).rollback
	      }
	    }
	    "not close the connection" in new TransactionalEnvironment{
	      jdbcTransaction.inTransaction{con => 
	        try{
	          jdbcTransaction.inTransaction(_=>throw new NotImplementedException)
	          fail
	        }catch{
	          case _:NotImplementedException =>
		        verify(connection,never()).close
	        }
	      }
	    }
	  }

	}
}