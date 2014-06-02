package org.hittepit.smapapi.mapper

import scala.language.implicitConversions
import java.sql.ResultSet
import java.sql.Connection
import scala.annotation.tailrec
import org.hittepit.smapapi.transaction.JdbcTransaction
import org.hittepit.smapapi.transaction.TransactionContext

class Projection

trait Mapper[E, I] { this: JdbcTransaction =>
  val tableName: String
  val pk: PrimaryKeyColumnDefinition[E,I]

  implicit def columnToValue[P](c: ColumnDefinition[E, P])(implicit rs: ResultSet) = c.value
  
  def column[E,P](n: String, st: SqlType[P], g: E => P) = new ColumnDefinition[E,P]{val name=n; val sqlType= st; val getter = g}

  def generatedPrimaryKey[E,I](n: String, st: SqlType[I], g: E => I, s: (E,I)=>E):PrimaryKeyColumnDefinition[E,I] = new AutoGeneratedColumn[E,I]{
      val name=n; val sqlType= st; val getter = g
      val setter = s
  }

  val insertable: List[ColumnDefinition[E, _]]
  val updatable: List[ColumnDefinition[E, _]]

  lazy val insertSqlString = {
    val columns = insertable.map(_.name).mkString(",")

    "insert into " + tableName + " (" + columns + ") values (" + (List.fill(insertable.size)("?")).mkString(",") + ")"
  }
  
  lazy val updateSqlString = {
    "update "+tableName+" set "+updatable.map(_.name+"=?").mkString(",")+" where "+pk.name+"=?"
  }

//  private def mapResultSet(rs: ResultSet, mapper: ResultSet => E): List[E] = {
//    @tailrec
//    def innerMap(acc: List[E]): List[E] = if (rs.next()) {
//      innerMap(mapper(rs) :: acc)
//    } else {
//      acc
//    }
//
//    innerMap(Nil)
//  }

  def select(condition:Condition):List[E]= readOnly{con =>
    select(None,Some(condition)) map (mapping(_))
  }
  
  //TODO gérer les exception
  def select(projection:Option[Projection],condition:Option[Condition]):ResultSetMapper= readOnly{con =>
    val sql = "select "+
    (projection match {
      case None => "*"
      case Some(p) => throw new NotImplementedError
    }) +
    " from "+tableName+
    (condition match {
      case None => ""
      case Some(c) => " where "+c.sqlString
    })
    val ps = con.prepareStatement(sql)
    condition match {
      case Some(c) => c.setParameter(0, ps)
      case None =>
    }
    new ResultSetMapper(ps.executeQuery())
  }

  def mapping(implicit rs: ResultSet): E

  class ResultSetMapper(rs:ResultSet){
	  def map[T](f:ResultSet => T):List[T]= {
	    def innerMap(acc:List[T]):List[T] = if(rs.next()){
	      innerMap(acc:::List(f(rs)))
	    } else {
	      acc
	    }
	    
	    innerMap(Nil)
	  }
  }
  
  def insert(entity: E): E = inTransaction { connection =>
    implicit val ps = connection.prepareStatement(insertSqlString)
    insertable.zipWithIndex.foreach { _ match{
      case (column,index) =>
      column.setValue(index + 1, entity)(ps)
    }}
    ps.executeUpdate()

    pk match {
      case key:AutoGeneratedColumn[E,I] =>
        if(getId(entity) != None){
          throw new IllegalArgumentException("The entity already has a pk. May you meant 'update', not insert.")
        }
        
	    val rs = ps.getGeneratedKeys()
	    rs.next
	    val i = key.value(1)(rs)
	    key.setter(entity,i)
	    
      case key:PrimaryKeyColumnDefinition[E,I] => throw new NotImplementedError
	    
    }
  }
  
  private def getId(e:E):I = pk.getter(e)
  
  def update(entity:E):Unit = inTransaction{connection =>
    if(pk.value(entity) == None) throw new IllegalArgumentException("The entity has no Id")
    val ps = connection.prepareStatement(updateSqlString)
    updatable.zipWithIndex.foreach{ _ match{
      case (column,index) => column.setValue(index+1, entity)(ps)
    }}
    pk.setValue(updatable.size+1, entity)(ps)
    val n = ps.executeUpdate
    if(n==0) throw new IllegalArgumentException("The object you're trying to update is transient")
    if(n>1) throw new ConfigurationException("Not unique id in database. The id you defined is not a real PK and some values are duplicated in database")
  }

  def findAll: List[E] = readOnly { connection =>
    val rs = connection.prepareStatement("select * from " + tableName).executeQuery
    new ResultSetMapper(rs). map(mapping(_))
  }

  def find(id: Any): Option[E] = readOnly { connection =>
    val sql = "select * from " + tableName + " where " + pk.name + " = ?"
    val ps = connection.prepareStatement(sql)
    pk.sqlType.setParameter(1, Some(id).asInstanceOf[I])(ps) //TODO adpater à toutes les PK match sur generated
    val rs = ps.executeQuery()
    new ResultSetMapper(rs). map(mapping(_)) match {
      case List(t) => Some(t)
      case Nil => None
      case _ => throw new Exception("PLus que une réponse") //TODO
    }
  }
}
