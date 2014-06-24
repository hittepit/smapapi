package org.hittepit.smapapi.core

import java.sql.Connection

import org.apache.commons.dbcp.BasicDataSource
import org.scalatest.BeforeAndAfter
import org.scalatest.MustMatchers
import org.scalatest.WordSpec

class TestExecuteInSession extends WordSpec with BeforeAndAfter with MustMatchers {
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
    st.addBatch("create table TEST (id integer auto_increment,name varchar(20),valeur integer auto_increment, PRIMARY KEY(id));")
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

  "The execute method" when {
    "called with an update request" must {
      "update the concerned rows" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        session.execute("update TEST set name=? where id=?",List(Param("Updated",NotNullableString),Param(1,NotNullableInt)))
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, 1)
        val rs = ps.executeQuery()
        rs.next
        rs.getString("name") must be("Updated")
        session.rollback
        session.close
      }
      "return the number of affected rows" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val updatedRows = session.execute("update TEST set name=? where id=?",List(Param("Updated",NotNullableString),Param(1,NotNullableInt)))

        updatedRows must be(1)
        
        session.rollback
        session.close
      } 
      "return 0 if no row was affacted" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val updatedRows = session.execute("update TEST set name=? where id=?",List(Param("Updated",NotNullableString),Param(1000,NotNullableInt)))

        updatedRows must be(0)
        
        session.rollback
        session.close
      }
    }
    "called with an delete request" must {
      "delete the concerned rows" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        session.execute("delete from TEST where id=?",List(Param(1,NotNullableInt)))
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, 1)
        val rs = ps.executeQuery()
        rs.next must be(false)
        session.rollback
        session.close
      }
      "return the number of affected rows" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val affectedRows = session.execute("delete from TEST where id=?",List(Param(1,NotNullableInt)))
        
        affectedRows must be(1)
        
        session.rollback
        session.close
      } 
      "return 0 if no row was affacted" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val affectedRows = session.execute("delete from TEST where id=?",List(Param(1000,NotNullableInt)))
        
        affectedRows must be(0)
        
        session.rollback
        session.close
      }
    }
    "called with an insert request" must {
      "insert a new row" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        session.execute("insert into TEST (id,name) values (?,?)",List(Param(1000,NotNullableInt),Param("hello",NotNullableString)))
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, 1000)
        val rs = ps.executeQuery()
        rs.next
        rs.getString("name") must be("hello")
        rs.getInt("id") must be(1000)
        session.rollback
        session.close
      }
      "return the number of affected rows" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val n = session.execute("insert into TEST (id,name) values (?,?)",List(Param(1001,NotNullableInt),Param("hello again",NotNullableString)))
        
        session.rollback
  
        session.close
        
        n must be(1)
      } 
    }
  }

}