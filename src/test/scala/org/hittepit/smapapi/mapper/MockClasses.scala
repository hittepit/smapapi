package org.hittepit.smapapi.mapper

import org.hittepit.smapapi.transaction.JdbcTransaction
import org.hittepit.smapapi.transaction.TransactionManager
import org.apache.commons.dbcp.BasicDataSource
import java.sql.ResultSet
import org.slf4j.LoggerFactory
import java.sql.Connection
import org.hittepit.smapapi.test.JdbcTestTransaction
import org.hittepit.smapapi.core.Column
import org.hittepit.smapapi.core.OptionalStringProperty
import org.hittepit.smapapi.core.OptionalIntProperty
import org.hittepit.smapapi.core.StringProperty
import org.hittepit.smapapi.core.DoubleProperty
import org.hittepit.smapapi.core.result.Row

case class Book(id: Option[Int], isbn: String, title: String, author: Option[String], price:Double)

class BookMapper(val transactionManager:TransactionManager) extends Mapper[Book, Option[Int]] with JdbcTransaction {
  val tableName = "BOOK"
  val pk = PrimaryKey(Column("id", OptionalIntProperty) <~ (_.id)) ~> ((book:Book,id:Option[Int]) => new Book(id,book.isbn,book.title,book.author,book.price))
  val isbn = Column("isbn", StringProperty) <~ (_.isbn)
  val title = Column("title", StringProperty) <~ ((b:Book) => b.title)
  val author = Column("author", OptionalStringProperty) <~ (_.author)
  val price = Column("price",DoubleProperty) <~ (_.price)
  def mapping(row:Row) = Book(pk(row), isbn(row), title(row), author(row),price(row))
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
