package org.hittepit.smapapi.core

import org.scalatest.BeforeAndAfter
import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection
import org.hittepit.smapapi.mapper.NotNullableVarchar
import org.hittepit.smapapi.mapper.NullableInteger
import org.hittepit.smapapi.mapper.NotNullableInteger

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
    "called for auto-generated values" must {
      "return a value" in {
        val connection = ds.getConnection()
        val session = new Session(connection)
        
        val valueGenerated = session.insert("insert into TEST (name) values (?)",List(Param("toto",NotNullableVarchar)),Column("id",NotNullableInteger))
        
        connection.commit
        connection.close
  
        valueGenerated must be('defined)
      }
      
      "do the insertion and return the value of the id" in {
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
  }

}