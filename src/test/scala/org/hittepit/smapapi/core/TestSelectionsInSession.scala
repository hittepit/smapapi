package org.hittepit.smapapi.core

import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection
import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.MustMatchers
import org.hittepit.smapapi.core.result.Row

class TestSelectionsInSession extends WordSpec with BeforeAndAfter with MustMatchers{
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
    "called without mapper" must {
      "generate a QueryResult that can be mapped to generate ojects" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        
        val rows = session.select("select isbn,author from book where price > ?",List(Param(5.0,DoubleProperty)))
        
        val tuples = rows map {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        connection.close
        
        tuples.size must be(2)
        tuples must contain(("12312",Some("toto")))
        tuples must contain(("12313",None))
      }
    }
    
    "called with a mapper and a query that has results" must {
      "generate a list of objects" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        val mapper = {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        val tuples = session.list("select isbn,author from book where price > ?",List(Param(5.0,DoubleProperty)),mapper)
        
        connection.close
        
        tuples.size must be(2)
        tuples must contain(("12312",Some("toto")))
        tuples must contain(("12313",None))
      }
    }
    
    "called with a mapper and a query that results in nothing" must {
      "generate an empty list" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        val mapper = {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        val tuples = session.list("select isbn,author from book where price < ?",List(Param(0.0,DoubleProperty)),mapper)
        
        connection.close
        
        tuples must be(Nil)
      }
    }
  }
  
  "The unique method" when {
    "called with a request that returns one row" must{
      "return Some(object)" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        val mapper = {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        val tuple = session.unique("select isbn,author from book where id = ?",List(Param(1,IntProperty)),mapper)
        
        tuple must be(Some(("12312",Some("toto"))))
        connection.close
      }
    }
    "called with a request that returns no row" must{
      "return None" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        val mapper = {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        val tuple = session.unique("select isbn,author from book where id = ?",List(Param(1000,IntProperty)),mapper)
        
        tuple must be(None)
        connection.close
      }
    }
    "called with a request that returns more than one row" must{
      "throw an exception" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with ReadOnlySession
        val mapper = {row:Row =>
          (row.getColumnValue("isbn", StringProperty),row.getColumnValue("author", OptionalStringProperty))
        }
        
        an [Exception] must be thrownBy (session.unique("select isbn,author from book where id > ?",List(Param(1,IntProperty)),mapper))
        
        connection.close
      }
    }
  }
}