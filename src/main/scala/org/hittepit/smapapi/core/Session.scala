package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import org.hittepit.smapapi.mapper.SqlType
import java.sql.ResultSetMetaData
import scala.collection.mutable.ArraySeq

case class Param[T](value: T, sqlType: SqlType[T])

class Column[T] private(n:Option[String], i:Option[Int], st:SqlType[T]){
  def this(n:String,st:SqlType[T]) = this(Some(n),None,st)
  def this(i:Int,st:SqlType[T]) = this(None, Some(i),st)
  
  val name = n
  val index = i
  val sqlType = st
}

object Column{
  def apply[T](name:String,sqlType:SqlType[T]) = new Column(name,sqlType)
  def apply[T](index:Int,sqlType:SqlType[T]) = new Column(index,sqlType)
  
  def unapply[T](c:Column[T]):Option[(_,SqlType[T])] = c.name match {
    case Some(n) => Some(n,c.sqlType)
    case None => Some(c.index.get,c.sqlType)
  }
}

class QueryResult(val rs: ResultSet) {
  def map[T](mapper: Row => T): List[T] = {
    def innerMap(acc: List[T]): List[T] = if (rs.next) {
      innerMap(acc ::: List(mapper(new Row(rs))))
    } else {
      acc
    }

    innerMap(Nil)
  }
}

class Row(resultSet: ResultSet) {
  def getColumnValue[T](index: Int, sqlType: SqlType[T]) = sqlType.getColumnValue(resultSet, Right(index))
  def getColumnValue[T](columnName: String, sqlType: SqlType[T]) = sqlType.getColumnValue(resultSet, Left(columnName))

}

class Session(val connection: Connection) {
  def select(sql: String, params: List[Param[_]]): QueryResult = {
    val ps = connection.prepareStatement(sql)

    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.sqlType.setColumnValue(index + 1, param.value, ps)
      }
    }

    new QueryResult(ps.executeQuery)
  }

  def select[T](sql: String, params: List[Param[_]], mapper: Row => T): Seq[T] = select(sql, params) map (mapper)

  def unique[T](sql: String, params: List[Param[_]], mapper: Row => T): Option[T] = select(sql, params) map (mapper) match {
    case Nil => None
    case List(t) => Some(t)
    case _ => throw new Exception("More than one result") //TODO exception 
  }

  def insert[T](sql: String, params: List[Param[_]], generatedId: Column[T]): Option[T] = insert(sql,params,Some(generatedId)) 
  def insert[T](sql: String, params: List[Param[_]]): Option[T] = insert(sql,params,None) 
  
  private def insert[T](sql: String, params: List[Param[_]], generatedId: Option[Column[T]]): Option[T] = {
    val ps = generatedId match {
      case Some(Column(name:String, sqlType)) => connection.prepareStatement(sql, Array(name))
      case Some(Column(index:Int, sqlType)) => throw new NotImplementedError
      case _ => connection.prepareStatement(sql)
    }

    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.sqlType.setColumnValue(index + 1, param.value, ps)
      }
    }

    ps.executeUpdate()
    generatedId match {
      case Some(Column(name:String, sqlType)) =>
        val queryResult = new QueryResult(ps.getGeneratedKeys())
        queryResult.map { row => row.getColumnValue(1, sqlType) } match {
          case List(m) => Some(m)
          case List() => None
          case _ => throw new Exception("Multiple generated objects")
        }
      case Some(Column(index:Int, sqlType)) => throw new NotImplementedError
      case None => None
    }
  }
}