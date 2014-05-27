package org.hittepit.smapapi.mapper

import scala.language.implicitConversions
import java.sql.ResultSet
import java.sql.Connection
import scala.annotation.tailrec
import org.hittepit.smapapi.transaction.JdbcTransaction

trait Mapper[T, X] { this: JdbcTransaction =>
  val tableName: String
  val pk: Option[ColumnDefinition[T, _, _]] = None

  implicit def columnToValue[S](c: ColumnDefinition[_, S, _])(implicit rs: ResultSet) = c.value

  def column[U, S](name: String, sqlType: SqlType[S], getter: T => U) = new ColumnDefinition(name, sqlType, getter, false)

  def generatedPrimaryKey[U, S](name: String, sqlType: SqlType[S], getter: T => U) = {
    new ColumnDefinition(name, sqlType, getter, true)
  }

  def map(implicit rs: ResultSet): T
  val insertable: List[ColumnDefinition[T, _, _]]
  val updatable: List[ColumnDefinition[T, _, _]]

  lazy val insertSqlString = {
    val columns = insertable.map(_.name).mkString(",")

    "insert into " + tableName + " (" + columns + ") values (" + (List.fill(insertable.size)("?")).mkString(",") + ")"
  }

  def mapResultSet(rs: ResultSet, mapper: ResultSet => T): List[T] = {
    @tailrec
    def innerMap(acc: List[T]): List[T] = if (rs.next()) {
      innerMap(mapper(rs) :: acc)
    } else {
      acc
    }

    innerMap(Nil)
  }

  def setId(id: X, entity: T): T

  def insert(entity: T): T = inTransaction { connection =>
    val ps = connection.prepareStatement(insertSqlString)
    insertable.zipWithIndex.foreach { el =>
      val column = el._1
      column.setValue(el._2 + 1, entity, ps)
    }
    ps.executeUpdate()

    //TODO gros brol....
    val rs = ps.getGeneratedKeys()
    rs.next
    val i = pk.get.value(1)(rs) match {
      case Some(n) => n
      case x => x
    }

    setId(i.asInstanceOf[X], entity)
  }

  def findAll: List[T] = readOnly { connection =>
    val rs = connection.prepareStatement("select * from " + tableName).executeQuery
    mapResultSet(rs, map(_))
  }

  def find(id: X): Option[T] = readOnly { connection =>
    pk match {
      case None => throw new Exception("pas de pk définie") //TODO
      case Some(key) =>
        val sql = "select * from " + tableName + " where " + key.name + " = ?"
        val ps = connection.prepareStatement(sql)
        key.sqlType.setParameter(1, ps, Some(id))
        val rs = ps.executeQuery()
        mapResultSet(rs, map(_)) match {
          case List(t) => Some(t)
          case Nil => None
          case _ => throw new Exception("PLus que une réponse") //TODO
        }
    }
  }
}
