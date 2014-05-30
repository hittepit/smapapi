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

case class Book(id: Option[Int], isbn: String, title: String, author: Option[String])

class BookMapper(val dataSource:DataSource) extends Mapper[Book, Option[Int]] with JdbcTransaction {
  val pk = id
  val tableName = "BOOK"
  def id = generatedPrimaryKey("id", NullableInteger, (b:Book) => b.id, (book:Book,id:Option[Int]) => new Book(id,book.isbn,book.title,book.author))
  def isbn = column("isbn", NotNullableVarchar, (b:Book) => b.isbn)
  def title = column("title", NotNullableVarchar, (b:Book) => b.title)
  def author = column("author", NullableVarchar, (b:Book) => b.author)
  def map(implicit rs: ResultSet) = Book(id, isbn, title, author)
  val insertable = List(isbn, title, author)
  val updatable = List(isbn, title, author)
}

class DataSource extends BasicDataSource {
  val logger = LoggerFactory.getLogger("datasource")
  this.setDriverClassName("org.h2.Driver")
  this.setUsername("h2")
  this.setUrl("jdbc:h2:mem:test;MVCC=TRUE")
}

class TestMapper extends WordSpec with MustMatchers with JdbcTestTransaction {
  val dataSource = new DataSource{}
  val mapper = new BookMapper(dataSource)

  inTransaction{ connection =>
	  var st = connection.createStatement
	  st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), PRIMARY KEY(id));")
	  st.addBatch("insert into book (id,isbn,title,author) values (1,'12312','Test','toto');")
	  st.addBatch("insert into book (id,isbn,title,author) values (2,'12313','Test2',null);")
	  st.executeBatch()
  }

  def invoked = afterWord("invoked")
  
  "An implemented mapper" when {
    "asked for insert sql String" must {
      "return a String ready for PreparedStatement" in {
        mapper.insertSqlString must be("insert into BOOK (isbn,title,author) values (?,?,?)")
      }
    }
    "asked for an update sql string" must {
      "return an update String ready for PreparedStatement" in {
        mapper.updateSqlString must be("update BOOK set isbn=?,title=?,author=? where id=?")
      }
    }
  }

  "The find method of a mapper" when invoked{
    "with an existing id with not null author" must {
      "return Some(b), b with Some(author)" in {
        val b = mapper.find(1)
        b match {
          case Some(book) =>
            book.id must be(Some(1))
            book.title must be("Test")
            book.isbn must be("12312")
            book.author must be(Some("toto"))
          case None => fail
        }
      }
    }
    "with an existing id with a null author" must {
      "return Some(b), author beigin None" in {
        val b = mapper.find(2)
        b match {
          case Some(book) =>
            book.id must be(Some(2))
            book.title must be("Test2")
            book.isbn must be("12313")
            book.author must be(None)
          case None => fail
        }
      }
    }
    "with a non existing id" must {
      "return None" in {
        val b = mapper.find(40)
        b match {
          case Some(book) => fail
          case None =>
        }
      }
    }
  }
  
  "The findAll method of a mapper" when {
    "invoked" must {
      "return all objects in the mapped table" in {
        val bs = mapper.findAll
        bs.size must be(2)
        bs must contain(Book(Some(1),"12312","Test",Some("toto")))
        bs must contain(Book(Some(2),"12313","Test2",None))
      }
    }
  }
  
  "The insert method" when invoked{
    "with an object with a non-defined autogenerated key" must {
      "return an instance of the same object, initialized the generated key" in withRollback{con =>
        val b = Book(None,"111","Nouveau",Some("ddd"))
        val b2 = mapper.insert(b)
        b2 must not be(null)
        b2.id.isDefined must be (true)
        b2.title must be("Nouveau")
        b2.author must be(Some("ddd"))
      }
      
      "insert the object in database" in withRollback{con =>
        val b = Book(None,"111","Nouveau",Some("ddd"))
        val b2 = mapper.insert(b)
        val id = b2.id.get
        val st = con.prepareStatement("select * from BOOK where id = ?")
        st.setInt(1, id)
        val rs = st.executeQuery()
        rs.next()
        rs.getString("title") must be("Nouveau")
      }
      
      "insert the object in database with null where properties are None" in withRollback{con =>
        val b = Book(None,"111","Nouveau",None)
        val b2 = mapper.insert(b)
        val id = b2.id.get
        val st = con.prepareStatement("select * from BOOK where id = ?")
        st.setInt(1, id)
        val rs = st.executeQuery()
        rs.next()
        rs.getString("title") must be("Nouveau")
        rs.getString("author")
        rs.wasNull() must be(true)
      }
    }
    
    "with an object initialized autogenerated key" must {
      "throw an IllegalArgumentException" in withRollback{con =>
        val b = Book(Some(50),"1414","Must fail",Some("one"))
        an [IllegalArgumentException] must be thrownBy(mapper.insert(b))
      }
    }
    
    "with an object with a key not auto-generated" must{
      "return a new version, that may be the same instance, of the object" in withRollback{con =>
        pending
      }
      "insert the object in database" in {
	      pending
      }    
    }
  }
  
  "The update method" when invoked{
    "with an object with an initialized id that exists in DB" must{
      "update the row in DB" in withRollback{con =>
        val bookToUpdate = Book(Some(1),"12312","Test1",Some("toto"))
        mapper.update(bookToUpdate)
        
        val ps = con.prepareStatement("select * from BOOK where id=?")
        ps.setInt(1, 1)
        val rs = ps.executeQuery()
        rs.next must be(true)
        rs.getInt("id") must be(1)
        rs.getString("title") must be("Test1")
        rs.getString("author") must be("toto")
        rs.next must be(false)
      }
    }
    
    "with an object with an initialized id that does not exist in DB" must{
      "throws an IllegalArgumentException" in withRollback{con =>
        val bookToUpdate = Book(Some(50),"12312","Test1",Some("toto"))
        an [IllegalArgumentException] must be thrownBy mapper.update(bookToUpdate)
      }
    }
    
    "with an object without id" must{
      "throw an IllegalArgumentException" in withRollback{con =>
        val bookToUpdate = Book(None,"12312","Test1",Some("toto"))
        an [IllegalArgumentException] must be thrownBy mapper.update(bookToUpdate)
      }
    }
  }
}