package org.hittepit.smapapi.mapper

import org.hittepit.smapapi.transaction.JdbcTransaction
import org.hittepit.smapapi.transaction.TransactionManager
import org.apache.commons.dbcp.BasicDataSource
import java.sql.ResultSet
import org.slf4j.LoggerFactory
import java.sql.Connection
import org.hittepit.smapapi.test.JdbcTestTransaction

case class Book(id: Option[Int], isbn: String, title: String, author: Option[String], price:Double)

class BookMapper(val transactionManager:TransactionManager) extends Mapper[Book, Option[Int]] with JdbcTransaction {
  val logger = LoggerFactory.getLogger(classOf[BookMapper])
  val tableName = "BOOK"
  val pk = PrimaryKey(Column("id", NullableInteger) <~ (_.id)) ~> ((book:Book,id:Option[Int]) => new Book(id,book.isbn,book.title,book.author,book.price))
  val isbn = Column("isbn", NotNullableVarchar) <~ (_.isbn)
  val title = Column("title", NotNullableVarchar) <~ ((b:Book) => b.title)
  val author = Column("author", NullableVarchar) <~ (_.author)
  val price = Column("price",NotNullableDouble) <~ (_.price)
  def mapping(implicit rs: ResultSet) = Book(pk, isbn, title, author,price)
  val insertable = List(isbn, title, author,price)
  val updatable = List(isbn, title, author,price)
}

class DataSource extends BasicDataSource {
  this.setDriverClassName("org.h2.Driver")
  this.setUsername("h2")
  this.defaultAutoCommit = false
  this.defaultTransactionIsolation = Connection.TRANSACTION_READ_COMMITTED
  this.setUrl("jdbc:h2:mem:test;MVCC=TRUE")
}
//
//trait MockBookMapper extends JdbcTestTransaction{
//  val ds = new DataSource{}
//  val transactionManager = new TransactionManager(ds)
//  val mapper = new BookMapper(transactionManager)
//}