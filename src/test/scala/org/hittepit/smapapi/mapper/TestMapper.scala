package org.hittepit.smapapi.mapper

import org.scalatest.WordSpec
import java.sql.ResultSet
import org.scalatest.GivenWhenThen
import org.scalatest.MustMatchers

case class Book(id:Option[Int],isbn:String, title:String, author:Option[String])

class BookMapper extends Mapper[Book,Int]{
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

	"An implemented mapper" when {
	  "asked for insert sql String" must {
	    "return a String ready for PreparedStatement" in {
	    	mapper.insertSqlString must be("insert into BOOK (isbn,title,author) values (?,?,?)")
	    }
	  }
	}
}