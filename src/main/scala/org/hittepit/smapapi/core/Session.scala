package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import org.hittepit.smapapi.mapper.SqlType
import java.sql.ResultSetMetaData

case class Param[T](value:T,sqlType:SqlType[T])

class QueryResult(val rs: ResultSet){
  def map[T](mapper: Row => T):List[T] = {
    def innerMap(acc:List[T]):List[T] = if(rs.next){
      innerMap(acc:::List(mapper(new Row(rs))))
    } else {
      acc
    }
    
    innerMap(Nil)
  }
}

class Row(resultSet:ResultSet){
  def getColumnValue[T](index:Int,sqlType:SqlType[T]) = sqlType.getColumnValue(resultSet,Right(index))
  def getColumnValue[T](columnName:String,sqlType:SqlType[T]) = sqlType.getColumnValue(resultSet,Left(columnName))

}

class Session(val connection:Connection) {
	def select(sql:String,params:List[Param[_]]):QueryResult = {
	  val ps = connection.prepareStatement(sql)
	  
	  params.zipWithIndex.foreach{_ match{
	    case (param:Param[_],index) => param.sqlType.setColumnValue(index+1,param.value,ps)
	  }}
	  
	  new QueryResult(ps.executeQuery)
	}
	
	def select[T](sql:String,params:List[Param[_]],mapper:Row => T):Seq[T] = select(sql,params) map (mapper)
	
	def unique[T](sql:String,params:List[Param[_]],mapper:Row => T):Option[T] = select(sql,params) map (mapper) match {
	  case Nil => None
	  case List(t) => Some(t)
	  case _ => throw new Exception("More than one result") //TODO exception 
	}
}