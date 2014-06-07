package org.hittepit.smapapi.core

import org.scalatest.BeforeAndAfter
import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection

class TestInsertInSession extends WordSpec with BeforeAndAfter with MustMatchers {
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
    st.addBatch("create table TEST (id integer auto_increment,name varchar(10),valeur integer auto_increment, PRIMARY KEY(id));")
    st.addBatch("insert into TEST (id,name) values (1,'Test1');")
    st.addBatch("insert into TEST (id,name) values (2,'Test2');")
    st.addBatch("insert into TEST (id,name) values (3,'Test3');")
    st.executeBatch()
    connection.commit()
    connection.close
  }

  after {
    val connection = ds.getConnection()
    var st = connection.createStatement
    st.addBatch("delete from TEST");
    st.addBatch("drop table TEST;")
    st.executeBatch()
    connection.commit()
    connection.close
  }

  "The insert method" when {
    "called with auto-generated id" must {
      "return the value of the generated id" in {
        val connection = ds.getConnection()
        val session = new Session(connection)
        
        val valueGenerated = session.insert("insert into TEST (name) values (?)",List(Param("toto",NotNullableVarchar)),Column("id",NotNullableInteger))
        
        connection.commit
        connection.close
  
        valueGenerated must be('defined)
      }
      
      "insert the data in db" in {
        val connection = ds.getConnection()
        val session = new Session(connection)
        
        val valueGenerated = session.insert("insert into TEST (name) values (?)",List(Param("hello",NotNullableVarchar)),Column("id",NotNullableInteger))
        
        connection.commit
  
        valueGenerated must be('defined)
        val id = valueGenerated.get
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, id)
        val rs = ps.executeQuery()
        rs.next
        rs.getString("name") must be("hello")
        rs.getInt("valeur")
        rs.wasNull must be(false)
        connection.close
      }
    }
    "called without auto-generated id" must {
      "return None" in {
        pending
      }
      "insert the data in DB" in {
        pending
      }
    }
  }
  
  "The execute method" when {
    "called with an update" must {
      "update the concerned rows" in {
        pending
      }
      "return the number of affected rows" in {
        pending
      } 
      "return 0 if no row was affacted" in {
        pending
      }
    }
    "called with an delete" must {
      "delete the concerned rows" in {
        pending
      }
      "return the number of affected rows" in {
        pending
      } 
      "return 0 if no row was affacted" in {
        pending
      }
    }
    "called with an insert" must {
      "insert a new row" in {
        pending
      }
      "return the number of affected rows" in {
        pending
      } 
    }
  }

}