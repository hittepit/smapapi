package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import org.scalatest.MustMatchers
import java.sql.DriverManager

case class Book(id:Option[Int],isbn:String, title:String, author:Option[String])

class BookMapper extends Mapper[Book,Int]{
  override val pk = Some(id)
  val tableName="BOOK"
  def id = generatedPrimaryKey("id",Nullable(Integer),_.id)
  def isbn = column("isbn",Varchar, _.isbn)
  def title = column("title",NotNullable(Varchar),_.title)
  def author = column("author", Nullable(Varchar),_.author)
  def map(implicit rs:ResultSet) = Book(id,isbn,title,author)
  val insertable = List(isbn,title,author)
  val updatable =  List(isbn,title,author)
}

class TestMapper extends WordSpec with MustMatchers{
  	val mapper = new BookMapper
  	Class.forName("org.h2.Driver")
  	
  	def getConnection = {
  		val c = DriverManager.getConnection("jdbc:h2:mem:test1")
	  	var st = c.prepareStatement("create table BOOK (id integer,isbn varchar(10),title varchar(20),author varchar(20))")
	  	st.execute()
	  	st.close
	  	st = c.prepareStatement("insert into book (id,isbn,title,author) values (1,'12312','Test','toto')")
	  	st.execute()
	  	st.close
	  	c
  	}

	"An implemented mapper" when {
	  "asked for insert sql String" must {
	    "return a String ready for PreparedStatement" in {
	    	mapper.insertSqlString must be("insert into BOOK (isbn,title,author) values (?,?,?)")
	    }
	  }
	  
	  "find" must {
	    "return Some(b)" in {
	      val b = mapper.find(1)(getConnection)
	      b match {
	        case Some(book) => book.id must be(Some(1))
	        					book.title must be("Test")
	        					book.isbn must be("12312")
	        					book.author must be(Some("toto"))
	        case None => fail
	      }
	    }
	  }
	}
}