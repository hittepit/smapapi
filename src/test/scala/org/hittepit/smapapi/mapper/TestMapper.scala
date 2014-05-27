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

case class Book(id: Option[Int], isbn: String, title: String, author: Option[String])

class BookMapper(val transactionManager:TransactionManager) extends Mapper[Book, Int] with JdbcTransaction {
  override val pk = Some(id)
  val tableName = "BOOK"
  def id = generatedPrimaryKey("id", Nullable(Integer), _.id)
  def isbn = column("isbn", Varchar, _.isbn)
  def title = column("title", NotNullable(Varchar), _.title)
  def author = column("author", Nullable(Varchar), _.author)
  def map(implicit rs: ResultSet) = Book(id, isbn, title, author)
  def setId(id:Int,book:Book) = new Book(Some(id),book.isbn,book.title,book.author)
  val insertable = List(isbn, title, author)
  val updatable = List(isbn, title, author)
}

class DataSource extends BasicDataSource {
  val logger = LoggerFactory.getLogger("datasource")
  this.setDriverClassName("org.h2.Driver")
  this.setUsername("h2")
  this.setUrl("jdbc:h2:mem:test;MVCC=TRUE")
}

class TestMapper extends WordSpec with MustMatchers {
  val mapper = new BookMapper(new TransactionManager{val dataSource = new DataSource()})
  val datasource = new DataSource{}

  val c = datasource.getConnection
  var st = c.createStatement
  st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), PRIMARY KEY(id));")
  st.addBatch("insert into book (id,isbn,title,author) values (1,'12312','Test','toto');")
  st.addBatch("insert into book (id,isbn,title,author) values (2,'12313','Test2',null);")
  st.executeBatch()
  st.close
  c.close

  "An implemented mapper" when {
    "asked for insert sql String" must {
      "return a String ready for PreparedStatement" in {
        mapper.insertSqlString must be("insert into BOOK (isbn,title,author) values (?,?,?)")
      }
    }
  }

  "The find method of a mapper" when {
    "called with an existing id with not null author" must {
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
    "called with an existing id with a null author" must {
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
    "called with a non existing id" must {
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
    "called" must {
      "return all objects in the mapped table" in {
        val bs = mapper.findAll
        bs.size must be(2)
        bs must contain(Book(Some(1),"12312","Test",Some("toto")))
        bs must contain(Book(Some(2),"12313","Test2",None))
      }
    }
  }
  
  "The insert method of a mapper" when {
    "called with a transient object" must {
      "return a new object" in {
        val b = Book(None,"111","Nouveau",Some("ddd"))
        val b2 = mapper.insert(b)
        b2 must not be(null)
        b2.id.isDefined must be (true)
        b2.title must be("Nouveau")
        b2.author must be(Some("ddd"))
        
        val id = b2.id.get
        val st = datasource.getConnection().prepareStatement("select * from BOOK where id = ?")
        st.setInt(1, id)
        val rs = st.executeQuery()
        rs.next()
        rs.getString("title") must be("Nouveau")
        
        //TODO close des connexions
      }
    }
  }
}