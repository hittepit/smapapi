package org.hittepit.smapapi.mapper

import scala.language.implicitConversions
import java.sql.ResultSet
import java.sql.Connection
import scala.annotation.tailrec
import org.hittepit.smapapi.transaction.JdbcTransaction

trait Mapper[E, I] { this: JdbcTransaction =>
  val tableName: String
  val pk: PrimaryKeyColumnDefinition[E,I]

  implicit def columnToValue[P](c: ColumnDefinition[E, P])(implicit rs: ResultSet) = c.value

  def column[E,P](n: String, st: SqlType[P], g: E => P) = new ColumnDefinition[E,P]{val name=n; val sqlType= st; val getter = g}

  def generatedPrimaryKey[E,I](n: String, st: SqlType[I], g: E => I, s: (E,I)=>E):PrimaryKeyColumnDefinition[E,I] = new AutoGeneratedColumn[E,I]{
      val name=n; val sqlType= st; val getter = g
      val setter = s
  }

  def map(implicit rs: ResultSet): E
  val insertable: List[ColumnDefinition[E, _]]
  val updatable: List[ColumnDefinition[E, _]]

  lazy val insertSqlString = {
    val columns = insertable.map(_.name).mkString(",")

    "insert into " + tableName + " (" + columns + ") values (" + (List.fill(insertable.size)("?")).mkString(",") + ")"
  }
  
  lazy val updateSqlString = {
    "update "+tableName+" set "+updatable.map(_.name+"=?").mkString(",")+" where "+pk.name+"=?"
  }

  def mapResultSet(rs: ResultSet, mapper: ResultSet => E): List[E] = {
    @tailrec
    def innerMap(acc: List[E]): List[E] = if (rs.next()) {
      innerMap(mapper(rs) :: acc)
    } else {
      acc
    }

    innerMap(Nil)
  }

//  def insert(entity: T): T = inTransaction { connection =>
//    val ps = connection.prepareStatement(insertSqlString)
//    insertable.zipWithIndex.foreach { el =>
//      val column = el._1
//      column.setValue(el._2 + 1, entity, ps)
//    }
//    ps.executeUpdate()
//
//    //TODO gros brol...., travail avec les clé non générées
//    pk match {
//      case Some(key:AutoGeneratedColumn[T,_,X]) =>
//	    val rs = ps.getGeneratedKeys()
//	    rs.next
//	    val i = key.value(1)(rs)
//	    
//	    println("------------->"+key.setter)
//	    key.setter(entity,i.asInstanceOf[X])
//	    
//      case Some(key) => throw new NotImplementedError
//	    
//      case None => throw new Exception //TODO erreur
//    }
//  }
//  
//  def update(entity:T):Unit = inTransaction{connection =>
//    val ps = connection.prepareStatement(updateSqlString)
//    updatable.zipWithIndex.foreach{ _ match{
//      case (column,index) => column.setValue(index, entity, ps)
//    }}
//    pk.get.setValue(updatable.size+1, entity, ps)
//    ps.executeUpdate
//  }
//
//  def findAll: List[T] = readOnly { connection =>
//    val rs = connection.prepareStatement("select * from " + tableName).executeQuery
//    mapResultSet(rs, map(_))
//  }
//
//  def find(id: X): Option[T] = readOnly { connection =>
//    pk match {
//      case None => throw new Exception("pas de pk définie") //TODO
//      case Some(key) =>
//        val sql = "select * from " + tableName + " where " + key.name + " = ?"
//        val ps = connection.prepareStatement(sql)
//        key.sqlType.setParameter(1, ps, Some(id))
//        val rs = ps.executeQuery()
//        mapResultSet(rs, map(_)) match {
//          case List(t) => Some(t)
//          case Nil => None
//          case _ => throw new Exception("PLus que une réponse") //TODO
//        }
//    }
//  }
}
