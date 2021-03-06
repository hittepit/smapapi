package org.hittepit.smapapi.mapper

import scala.language.implicitConversions
import scala.annotation.tailrec
import org.hittepit.smapapi.transaction.JdbcTransaction
import org.hittepit.smapapi.transaction.TransactionContext
import org.hittepit.smapapi.core.session.Param
import org.hittepit.smapapi.core.result.QueryResult
import org.hittepit.smapapi.core.result.Row
import org.hittepit.smapapi.core.session.Column
import org.hittepit.smapapi.core.PropertyType

trait Mapper[E, I] { this: JdbcTransaction =>
  object PrimaryKey{
    def apply[P](cdef:ColumnDefinition[E,P]) = new PrimaryKeyColumnDefinition(cdef.name,cdef.PropertyType,cdef.getter)
  }


  val tableName: String
  val pk: PrimaryKeyColumnDefinition[E,I]

  implicit def columnToValue[P](c: ColumnDefinition[E, P]) = c.value
  
  implicit def columnToColumnDef[P](c:Column[P]):ColumnDef[E,P] = c match{
    case Column(name:String,propertyType) => new ColumnDef(name,c.PropertyType)
    case _ => throw new Exception("Conversion impossible") //TODO
  }
  
  def column[E,P](n: String, st: PropertyType[P], g: E => P) = new ColumnDefinition[E,P](n,st,g)

  def generatedPrimaryKey[E,I](n: String, st: PropertyType[I], g: E => I, s: (E,I)=>E):PrimaryKeyColumnDefinition[E,I] = new AutoGeneratedColumn[E,I](n,st,g,s)

  val insertable: List[ColumnDefinition[E, _]]
  val updatable: List[ColumnDefinition[E, _]]

  lazy val insertSqlString = {
    val columns = insertable.map(_.name).mkString(",")

    "insert into " + tableName + " (" + columns + ") values (" + (List.fill(insertable.size)("?")).mkString(",") + ")"
  }
  
  lazy val updateSqlString = {
    "update "+tableName+" set "+updatable.map(_.name+"=?").mkString(",")+" where "+pk.name+"=?"
  }

  def select(condition:Condition):List[E]= readOnly{con =>
    select(None,Some(condition)) map (mapping(_))
  }
  
  //TODO gérer les exceptions?
  def select(projection:Option[Projection],condition:Option[Condition]):QueryResult= readOnly{session =>
    val sql = "select "+
    (projection match {
      case None => "*"
      case Some(p) => p.sqlString
    }) +
    " from "+tableName+
    (condition match {
      case None => ""
      case Some(c) => " where "+c.sqlString
    })
    logger.debug(sql)
    val params = condition match {
      case Some(c) => c.getParams
      case None => Nil
    }
    session.select(sql, params)
  }
  
  def select(sql:String,params:List[Param[_]]): QueryResult = readOnly {session =>
    session.select(sql, params)
  }
  
  def executeSql(sql:String,params:List[Param[_]]) = throw new NotImplementedError //TODO
  
  def mapping(row:Row): E

  def insert(entity: E): E = inTransaction { session =>
    val params = insertable.map{c =>
      Param(c.getter(entity),c.PropertyType.asInstanceOf[PropertyType[Any]])
    }
    

    pk match {
      case key:AutoGeneratedColumn[E,I] =>
        if(getId(entity) != None){
          throw new IllegalArgumentException("The entity already has a pk. May you meant 'update', not insert.")
        }
        session.insert(insertSqlString, params,Column(key.name,key.PropertyType)) match {
          case Some(id) => key.setter(entity,id)
          case None => throw new Exception("No generated id received") //TODO
        }

	    
      case key:PrimaryKeyColumnDefinition[E,I] => throw new NotImplementedError
	    
    }
  }
  
  private def getId(e:E):I = pk.getter(e)
  
  def update(entity:E):Unit = inTransaction{session =>
    if(pk.value(entity) == None) throw new IllegalArgumentException("The entity has no Id")
    val params = updatable.map{c =>
      Param(c.getter(entity),c.PropertyType.asInstanceOf[PropertyType[Any]])
    }:::List(Param(pk.value(entity),pk.PropertyType))
    
    val n = session.execute(updateSqlString, params)
    if(n==0) throw new IllegalArgumentException("The object you're trying to update is transient")
    if(n>1) throw new ConfigurationException("Not unique id in database. The id you defined is not a real PK and some values are duplicated in database")
  }

  def findAll: List[E] = readOnly { session =>
    val queryResult = session.select("select * from " + tableName,List())
    queryResult map(mapping(_))
  }

  def find(id: I): Option[E] = readOnly { session =>
    val sql = "select * from " + tableName + " where " + pk.name + " = ?"
    val params = List(Param(id,pk.PropertyType))
    session.unique(sql,params,mapping)
  }
}
