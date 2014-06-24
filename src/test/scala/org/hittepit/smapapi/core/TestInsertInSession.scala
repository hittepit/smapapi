package org.hittepit.smapapi.core

import java.sql.Connection

import org.apache.commons.dbcp.BasicDataSource
import org.scalatest.BeforeAndAfter
import org.scalatest.MustMatchers
import org.scalatest.WordSpec

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

  "The insert method" when {
    "called with auto-generated id" must {
      "return the value of the generated id" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val valueGenerated = session.insert("insert into TEST (name) values (?)",List(Param("toto",StringProperty)),Column("id",IntProperty))
  
        valueGenerated must be('defined)
        
        session.rollback
        session.close
      }
      
      "insert the data in db" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        val valueGenerated = session.insert("insert into TEST (name) values (?)",List(Param("hello",StringProperty)),Column("id",IntProperty))
        
        valueGenerated must be('defined)
        val id = valueGenerated.get
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, id)
        val rs = ps.executeQuery()
        rs.next
        rs.getString("name") must be("hello")
        rs.getInt("valeur")
        rs.wasNull must be(false)
        
        session.rollback
        session.close
      }
    }
    "called without auto-generated id" must {
      "insert the data in DB" in {
        val connection = ds.getConnection()
        val session = new Session(connection) with UpdatableSession
        
        session.insert("insert into TEST (id,name) values (?,?)",List(Param(1000,IntProperty),Param("hello again",StringProperty)))
        
        val ps = connection.prepareStatement("select * from test where id = ?")
        ps.setInt(1, 1000)
        val rs = ps.executeQuery()
        rs.next
        rs.getString("name") must be("hello again")
        rs.getInt("id") must be(1000)
        
        session.rollback
        session.close
      }
    }
  }

}