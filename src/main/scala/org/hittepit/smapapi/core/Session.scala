package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import org.hittepit.smapapi.mapper.SqlType
import java.sql.ResultSetMetaData
import scala.collection.mutable.ArraySeq

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
	
	def insert(sql:String, params:List[Param[_]], generatedColumns:List[(String,SqlType[_])]) = {
	  val ps = connection.prepareStatement(sql,generatedColumns.map(_._1).toArray)
	  
	  params.zipWithIndex.foreach{_ match{
	    case (param:Param[_],index) => param.sqlType.setColumnValue(index+1,param.value,ps)
	  }}
	  
	  ps.executeQuery()
	  
	  val queryResult = new QueryResult(ps.getGeneratedKeys())
	  
	  queryResult.map{row =>
		  def innerMap(columns:List[(String,SqlType[_])],acc:Map[String,Any]):Map[String,Any] = columns match {
		    case Nil => acc
		    case c::cs => innerMap(cs, acc + (c._1 -> row.getColumnValue(c._1, c._2)))
		  } 
	    
		  innerMap(generatedColumns,Map())
	  } match {
	    case List(m) => m
	    case List() => Map()
	    case _ => throw new Exception("Multiple generated objects")
	  }
	}
}