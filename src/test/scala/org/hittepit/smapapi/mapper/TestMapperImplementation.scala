package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import org.scalatest.MustMatchers
import java.sql.DriverManager
import java.util.logging.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.dbcp.BasicDataSource
import org.hittepit.smapapi.transaction.TransactionManager
import org.hittepit.smapapi.transaction.JdbcTransaction
import org.hittepit.smapapi.test.JdbcTestTransaction
import java.sql.Connection
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter

class TestMapperImplementation extends WordSpec with MustMatchers with MockitoSugar with JdbcTestTransaction with BeforeAndAfter {
  val ds = new DataSource{}
  val transactionManager = TransactionManager(ds)
  val mapper = new BookMapper(transactionManager)

  before{
	  inTransaction{ session =>
		  var st = session.connection.createStatement
		  st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), price double, PRIMARY KEY(id));")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (1,'12312','Test','toto',10.40);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (2,'12313','Test2',null,5.60);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (3,'12314','Test3','toto',2.25);")
		  st.executeBatch()
	  }
  }
  
  after{
    inTransaction{session =>
		  var st = session.connection.createStatement
		  st.addBatch("delete from book");
	      st.addBatch("drop table BOOK;")
		  st.executeBatch()
    }
  }

  def invoked = afterWord("invoked")
  
  "An implemented mapper" when {
    "asked for insert sql String" must {
      "return a String ready for PreparedStatement" in {
        mapper.insertSqlString must be("insert into BOOK (isbn,title,author,price) values (?,?,?,?)")
      }
    }
    "asked for an update sql string" must {
      "return an update String ready for PreparedStatement" in {
        mapper.updateSqlString must be("update BOOK set isbn=?,title=?,author=?,price=? where id=?")
      }
    }
  }

}