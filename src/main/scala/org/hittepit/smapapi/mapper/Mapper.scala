package org.hittepit.smapapi.mapper

import scala.language.implicitConversions
import java.sql.ResultSet
import java.sql.Connection
import scala.annotation.tailrec

trait Mapper[T,X] {
  val tableName: String
  var pk:Option[ColumnDefinition[T,_,_]] = None
  
  implicit def columnToValue[S](c: ColumnDefinition[_, S, _])(implicit rs: ResultSet) = c.value
  
  def column[U, S](name: String, sqlType: SqlType[S], getter: T => U) = new ColumnDefinition(name, sqlType, getter, false)
  
  def generatedPrimaryKey[U, S](name: String, sqlType: SqlType[S], getter: T => U) = {
    val cd = new ColumnDefinition(name, sqlType, getter, true)
    pk = Some(cd)
    cd
  }

  def map(implicit rs: ResultSet): T
  val insertable: List[ColumnDefinition[T, _, _]]
  val updatable: List[ColumnDefinition[T, _, _]]

  def insertSqlString = {
    val columns = insertable.map(_.name).mkString(",")

    "insert into " + tableName + " (" + columns + ") values (" + (List.fill(insertable.size)("?")).mkString(",")+ ")"
  }
  
  def mapResultSet(rs:ResultSet, mapper:ResultSet => T):List[T] = {
    @tailrec
    def innerMap(acc:List[T]):List[T] = if(rs.next()){
      innerMap(mapper(rs) :: acc)
    } else {
      acc
    }
    
    innerMap(Nil)
  }
  
  def findAll(implicit connection:Connection):List[T] = {
    val rs = connection.prepareStatement("select * from "+tableName).executeQuery
    mapResultSet(rs, map(_))
  }
  
  def find(id:X)(implicit connection:Connection):Option[T] = pk match {
    case None => throw new Exception("pas de pk définie") //TODO
    case Some(key) =>
	    val sql = "select * from "+tableName+" where "+key.name+" = ?"
	    val ps = connection.prepareStatement(sql)
	    key.sqlType.setParameter(1,ps,Some(id))
    //TODO
    None
  }
}
