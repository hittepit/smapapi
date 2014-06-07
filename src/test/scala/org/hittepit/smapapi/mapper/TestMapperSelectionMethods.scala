package org.hittepit.smapapi.mapper

import org.hittepit.smapapi.test.JdbcTestTransaction
import org.scalatest.mock.MockitoSugar
import org.scalatest.MustMatchers
import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import org.hittepit.smapapi.mapper.Condition._
import java.sql.ResultSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.hittepit.smapapi.transaction.TransactionManager
import org.hittepit.smapapi.core.Param
import org.hittepit.smapapi.core.Row

class TestMapperSelectionMethods extends WordSpec with MustMatchers with MockitoSugar with JdbcTestTransaction with BeforeAndAfter {
  val logger = LoggerFactory.getLogger(classOf[TestMapperSelectionMethods])
  val ds = new DataSource{}
  val transactionManager = new TransactionManager(ds)
  val mapper = new BookMapper(transactionManager)

  before{
	  inTransaction{ session =>
		  var st = session.connection.createStatement
		  st.addBatch("create table BOOK (id integer auto_increment,isbn varchar(10),title varchar(20),author varchar(20), price double, PRIMARY KEY(id));")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (1,'12312','Test','toto',10.40);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (2,'12313','Test2',null,5.60);")
		  st.addBatch("insert into book (id,isbn,title,author,price) values (3,'12314','Test3','toto',2.25);")
		  st.executeBatch()
	  }
  }
  
  after{
    inTransaction{session =>
		  var st = session.connection.createStatement
		  st.addBatch("delete from book");
	      st.addBatch("drop table BOOK;")
		  st.executeBatch()
    }
  }

  def invoked = afterWord("invoked")

  "The find method of a mapper" when invoked {
    "with an existing id with not null author" must {
      "return Some(b), b with Some(author)" in {
        val b = mapper.find(Some(1))
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
        val b = mapper.find(Some(2))
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
        val b = mapper.find(Some(40))
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
        bs.size must be(3)
        bs must contain(Book(Some(1), "12312", "Test", Some("toto"), 10.40))
        bs must contain(Book(Some(2), "12313", "Test2", None, 5.60))
        bs must contain(Book(Some(3), "12314", "Test3", Some("toto"), 2.25))
      }
    }
  }

  "The select method" when invoked {
    "with a simple condition" must {
      "return a list of objects fulfilling the condition" in {
        val condition = new EqualsCondition(mapper.author, Some("toto"))
        val books = mapper.select(condition)
        books.size must be(2)
        books must contain(Book(Some(1), "12312", "Test", Some("toto"), 10.40))
        books must contain(Book(Some(3), "12314", "Test3", Some("toto"), 2.25))
      }
    }

    "with a compound condition" must {
      "return a list of objects fulfilling the condition" in {
        //Warning, eq used by ScalaTest matchers
        val condition = and(Condition.eq(mapper.author, Some("toto")), gt(mapper.price, 3.0))
        val books = mapper.select(condition)
        books.size must be(1)
        books must contain(Book(Some(1), "12312", "Test", Some("toto"), 10.40))
      }
    }

    "with a projection, a condition and a mapper" must {
      "return the list of mapped objects" in readOnly { con =>
        val condition = and(Condition.like(mapper.title, "Test%"), gt(mapper.price, 5.0))
        val projection = Projection((Some("t"), mapper.title), (Some("a"), mapper.author))
        val mapTuple: Row => (String, Option[String]) = r => {
          val title = r.getColumnValue("t",NotNullableVarchar)
          val author = r.getColumnValue("a",NullableVarchar)
          (title, author)
        }

        val tuples = mapper.select(Some(projection), Some(condition)) map (mapTuple)

        tuples.size must be(2)
        tuples must contain("Test", Some("toto"))
        tuples must contain("Test2", None)
      }
    }
    
    "with a sql string, a list of params and a mapper" must {
      "return a list of mapped objects" in readOnly {con =>
        val select = mapper.select("select sum(price) as total, author from book where author like ? group by author",List(Param("to%",NotNullableVarchar)))
        val tuples = select map { row =>
          (row.getColumnValue("total",NotNullableDouble),row.getColumnValue("author",NotNullableVarchar))
        }
        
        tuples.size must be(1)
        tuples must contain((12.65,"toto"))
      }
    }
  }

}