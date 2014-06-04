package org.hittepit.smapapi.mapper

import org.hittepit.smapapi.test.JdbcTestTransaction
import org.scalatest.mock.MockitoSugar
import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter

class TestMapperUpdateMethod extends WordSpec with MustMatchers with MockitoSugar with MockBookMapper with BeforeAndAfter with BeforeAndAfterAll{
  val logger = LoggerFactory.getLogger(classOf[TestMapperUpdateMethod])

  before{
	  inTransaction{ connection =>
		  var st = connection.createStatement
		  st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), price double, PRIMARY KEY(id));")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (1,'12312','Test','toto',10.40);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (2,'12313','Test2',null,5.60);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (3,'12314','Test3','toto',2.25);")
		  st.executeBatch()
	  }
  }
  
  after{
    inTransaction{connection =>
		  var st = connection.createStatement
		  st.addBatch("delete from book");
	      st.addBatch("drop table BOOK;")
		  st.executeBatch()
    }
  }

  override def afterAll(){
    println("------> Closing Datasource")
    ds.close()
  }

  def invoked = afterWord("invoked")

  "The update method" when invoked{
    "with an object with an initialized id that exists in DB" must{
      "update the row in DB" in withRollback{con =>
        val bookToUpdate = Book(Some(1),"12312","Test1",Some("toto"),7.5)
        mapper.update(bookToUpdate)
        
        val ps = con.prepareStatement("select * from BOOK where id=?")
        ps.setInt(1, 1)
        val rs = ps.executeQuery()
        rs.next must be(true)
        rs.getInt("id") must be(1)
        rs.getString("title") must be("Test1")
        rs.getString("author") must be("toto")
        rs.getDouble("price") must be(7.5)
        rs.next must be(false)
      }
    }
    
    "with an object with an initialized id that does not exist in DB" must{
      "throws an IllegalArgumentException" in withRollback{con =>
        val bookToUpdate = Book(Some(50),"12312","Test1",Some("toto"),7.5)
        an [IllegalArgumentException] must be thrownBy mapper.update(bookToUpdate)
        //TODO chek no update made
      }
    }
    
    "with an object without id" must{
      "throw an IllegalArgumentException" in withRollback{con =>
        val bookToUpdate = Book(None,"12312","Test1",Some("toto"),5.6)
        an [IllegalArgumentException] must be thrownBy mapper.update(bookToUpdate)
      }
    }
  }

}