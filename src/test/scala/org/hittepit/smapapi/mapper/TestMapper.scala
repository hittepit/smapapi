package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import org.scalatest.MustMatchers
import java.sql.DriverManager
import java.util.logging.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.dbcp2.BasicDataSource

case class Book(id: Option[Int], isbn: String, title: String, author: Option[String])

class BookMapper extends Mapper[Book, Int] {
  override val pk = Some(id)
  val tableName = "BOOK"
  def id = generatedPrimaryKey("id", Nullable(Integer), _.id)
  def isbn = column("isbn", Varchar, _.isbn)
  def title = column("title", NotNullable(Varchar), _.title)
  def author = column("author", Nullable(Varchar), _.author)
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

class TestMapper extends WordSpec with MustMatchers {
  val mapper = new BookMapper
  val datasource = new DataSource

  val c = datasource.getConnection
  var st = c.createStatement
  st.addBatch("create table BOOK (id integer,isbn varchar(10),title varchar(20),author varchar(20));")
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
        val b = mapper.find(1)(datasource.getConnection)
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
        val b = mapper.find(2)(datasource.getConnection)
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
        val b = mapper.find(40)(datasource.getConnection)
        b match {
          case Some(book) => fail
          case None =>
        }
      }
    }
  }
}