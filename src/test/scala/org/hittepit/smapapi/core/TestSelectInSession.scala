package org.hittepit.smapapi.core

import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import org.hittepit.smapapi.mapper.NotNullableDouble
import org.hittepit.smapapi.mapper.NotNullableVarchar
import org.hittepit.smapapi.mapper.NullableVarchar
import org.scalatest.MustMatchers

class TestSelectInSession extends WordSpec with BeforeAndAfter with MustMatchers{
  class DataSource extends BasicDataSource {
    this.setDriverClassName("org.h2.Driver")
    this.setUsername("h2")
    this.defaultAutoCommit = false
    this.defaultTransactionIsolation = Connection.TRANSACTION_READ_COMMITTED
    this.setUrl("jdbc:h2:mem:test;MVCC=TRUE")
  }

  val ds = new DataSource

  before {
    val connection = ds.getConnection()
    var st = connection.createStatement
    st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), price double, PRIMARY KEY(id));")
    st.addBatch("insert into book (id,isbn,title,author,price) values (1,'12312','Test','toto',10.40);")
    st.addBatch("insert into book (id,isbn,title,author,price) values (2,'12313','Test2',null,5.60);")
    st.addBatch("insert into book (id,isbn,title,author,price) values (3,'12314','Test3','toto',2.25);")
    st.executeBatch()
    connection.commit()
    connection.close
  }

  after {
    val connection = ds.getConnection()
    var st = connection.createStatement
    st.addBatch("delete from book");
    st.addBatch("drop table BOOK;")
    st.executeBatch()
    connection.commit()
    connection.close
  }

  "The select method" when {
    "called" must {
      "work" in {
        val connection = ds.getConnection()
        val session = new Session(connection)
        
        val rows = session.select("select isbn,author from book where price > ?",List(Param(5.0,NotNullableDouble)))
        
        val tuples = (rows map {row:Row =>
          (row.getColumnValue("isbn", NotNullableVarchar),row.getColumnValue("author", NullableVarchar))
        }).toList
        
        tuples.size must be(2)
        tuples must contain(("12312",Some("toto")))
        tuples must contain(("12313",None))
        
        connection.close
      }
    }
  }
}