package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import scala.collection.mutable.ArraySeq

/**
 * Définition d'un paramètre à injecter dans un PreparedStatement.
 * 
 * @constructor Crée un objet Param
 * @param value la valeur du paramètre
 * @param sqlType le type sql du paramètre
 * 
 */
case class Param[T](value: T, sqlType: SqlType[T])

/**
 * Définition d'un colonne. Elle permet de récupérer une valeur dans un ResultSet.
 * 
 * La colonne peut être définie soit par son index dans le ResultSet, soit par son nom.
 */
class Column[T] private(n:Option[String], i:Option[Int], st:SqlType[T]){
  /**
   * Crée une colonne pouvant être retrouvée via son nom
   * @param columnName le nom de la colonne
   * @param sqlType le SqlType correspondant à la colonne
   */
  def this(columnName:String,sqlType:SqlType[T]) = this(Some(columnName),None,sqlType)
  /**
   * Crée une colonne pouvant être retrouvée via son nom
   * @param columnIndex l'index de la colonne (à partir de 1)
   * @param sqlType le SqlType correspondant à la colonne
   */
  def this(columnIndex:Int,sqlType:SqlType[T]) = this(None, Some(columnIndex),sqlType)
  
  /** Le nom de la colonne dans le ResultSet, si défini*/
  val name = n
  /** L'index de la colonne dans le ResultSet, si défini*/
  val index = i
  /** Le type Sql de la colonne dans le ResultSet */
  val sqlType = st
}

/**
 * Contient les méthodes factory et les extracteurs de [[Column]].
 */
object Column{
  /**
   * Construit un objet [[Column]] à partir de son nom dans le ResultSet.
   * @param name nom de la colonne dans le ResultSet
   * @param sqlType type Sql de la colonne
   */
  def apply[T](name:String,sqlType:SqlType[T]) = new Column(name,sqlType)
  /**
   * Construit un objet [[Column]] à partir de son index (à partir de 1) dans le ResultSet.
   * @param index index de la colonne dans le ResultSet
   * @param sqlType type Sql de la colonne
   */
  def apply[T](index:Int,sqlType:SqlType[T]) = new Column(index,sqlType)
  
  /**
   * Extracteur pour la class [[Column]]
   * @param column l'objet [[Column]] à pattern matcher
   * @return une option contenant un tuple, 
   *  - (Int,SqlType) si c'est une colonne définie sur base de son index
   *  - (String,SqlType) si c'est une colonne définie sur base de son nom
   */
  def unapply[T](column:Column[T]):Option[(_,SqlType[T])] = column.name match {
    case Some(n) => Some(n,column.sqlType)
    case None => Some(column.index.get,column.sqlType)
  }
}

class QueryResult(val rs: ResultSet) {
  /**
   * Transforme un ResultSet en objets
   * @param mapper fonction
   */
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
  
  def execute(sql:String, params:List[Param[_]]):Int = {
    val ps = connection.prepareStatement(sql)
    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.sqlType.setColumnValue(index + 1, param.value, ps)
      }
    }
    ps.executeUpdate()
  }
  
  def commit() = connection.commit()
  def rollback() = connection.rollback()
  def close() = connection.close
}