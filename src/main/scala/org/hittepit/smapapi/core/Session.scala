package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import scala.collection.mutable.ArraySeq
import org.hittepit.smapapi.core.result.QueryResult
import org.hittepit.smapapi.core.result.Row

/**
 * Définition d'un paramètre à injecter dans un PreparedStatement.
 * 
 * @constructor Crée un objet Param
 * @param value la valeur du paramètre
 * @param PropertyType le type du paramètre
 * 
 */
class Param[T](val value: T, val propertyType: PropertyType[T])

object Param{
  def apply[T](value:T, propertyType: PropertyType[T]) = new Param(value,propertyType)
  
  def apply[T](value:T) = {
    val t:Class[T] = value.getClass.asInstanceOf[Class[T]]
    if(t == classOf[Some[_]]){
//      value match {
//        case Some(i) => println("-----------------> "+i.getClass)
//      }
      throw new Exception("Some ne fonctionne pas, utilisez Param(value,propertyType)") //TODO exception
    }
    new Param(value,PropertyTypes.propertyType(t))
  }
  
  def unapply[T](param:Param[T]):Option[(T,PropertyType[T])] = Some(param.value,param.propertyType)
}

/**
 * Définition d'un colonne. Elle permet de récupérer une valeur dans un ResultSet.
 * 
 * La colonne peut être définie soit par son index dans le ResultSet, soit par son nom.
 */
class Column[T] private(n:Option[String], i:Option[Int], st:PropertyType[T]){
  /**
   * Crée une colonne pouvant être retrouvée via son nom
   * @param columnName le nom de la colonne
   * @param PropertyType le PropertyType correspondant à la colonne
   */
  def this(columnName:String,PropertyType:PropertyType[T]) = this(Some(columnName),None,PropertyType)
  /**
   * Crée une colonne pouvant être retrouvée via son nom
   * @param columnIndex l'index de la colonne (à partir de 1)
   * @param PropertyType le PropertyType correspondant à la colonne
   */
  def this(columnIndex:Int,PropertyType:PropertyType[T]) = this(None, Some(columnIndex),PropertyType)
  
  /** Le nom de la colonne dans le ResultSet, si défini*/
  val name = n
  /** L'index de la colonne dans le ResultSet, si défini*/
  val index = i
  /** Le type de la valeur récupérée dans la colonne du ResultSet */
  val PropertyType = st
}

/**
 * Contient les méthodes factory et les extracteurs de [[Column]].
 */
object Column{
  /**
   * Construit un objet [[Column]] à partir de son nom dans le ResultSet.
   * @param name nom de la colonne dans le ResultSet
   * @param PropertyType type de la valeur de la colonne
   */
  def apply[T](name:String,PropertyType:PropertyType[T]) = new Column(name,PropertyType)
  /**
   * Construit un objet [[Column]] à partir de son index (à partir de 1) dans le ResultSet.
   * @param index index de la colonne dans le ResultSet
   * @param PropertyType type de la valeur de la colonne
   */
  def apply[T](index:Int,PropertyType:PropertyType[T]) = new Column(index,PropertyType)
  
  /**
   * Extracteur pour la class [[Column]]
   * @param column l'objet [[Column]] à pattern matcher
   * @return une option contenant un tuple, 
   *  - (Int,PropertyType) si c'est une colonne définie sur base de son index
   *  - (String,PropertyType) si c'est une colonne définie sur base de son nom
   */
  def unapply[T](column:Column[T]):Option[(_,PropertyType[T])] = column.name match {
    case Some(n) => Some(n,column.PropertyType)
    case None => Some(column.index.get,column.PropertyType)
  }
}

/**
 * Classe encapsulant une connexion et automatisant les échanges avec la base de données en masquant l'utilisation de JDBC.
 * @constructor Crée une session
 * @param connection la connexion encapsulée dans la session. Une session n'utilise qu'une seule et unique connexion.
 */
class Session(val connection: Connection) {
  /**
   * Méthode de base pour exécuter un SELECT SQL. Elle génère un ResulSet et l'encapsule dans un [[org.hittepit.smapapi.core.result.QueryResult QueryResult]].
   *  
   * @param sql String contenant la requête SQL qui sera utilisée pour créer un PreparedStatement. Les paramètres variables doivent donc être représenté par '?'
   * @param params Liste d'objet [[Param]] qui seront injectés dans le PreparedStatement.
   * @return un [[org.hittepit.smapapi.core.result.QueryResult QueryResult]]
   */
  def select(sql: String, params: List[Param[_]]): QueryResult = {
    val ps = connection.prepareStatement(sql)

    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.propertyType.setColumnValue(index + 1, param.value, ps)
      }
    }

    new QueryResult(ps.executeQuery)
  }

  /**
   * Méthode permettant d'exécuter un select et de retourner directement une liste d'objets de type T.
   * @param sql String contenant la requête SQL qui sera utilisée pour créer un PreparedStatement. Les paramètres variables doivent donc être représenté par '?'
   * @param params Liste d'objets [[Param]] qui seront injectés dans le PreparedStatement.
   * @param mapper une méthode qui transforme une [[org.hittepit.smapapi.core.result.Row Row]] en objet T
   * @return une liste d'objets T
   */
  def select[T](sql: String, params: List[Param[_]], mapper: Row => T): Seq[T] = select(sql, params) map (mapper)


  /**
   * Méthode permettant d'exécuter un select et de retourner un objet T unique. Si la requête retourne plusieurs rows, une exception est lancée.
   * @param sql String contenant la requête SQL qui sera utilisée pour créer un PreparedStatement. Les paramètres variables doivent donc être représenté par '?'
   * @param params Liste d'objets [[Param]] qui seront injectés dans le PreparedStatement.
   * @param mapper une méthode qui transforme une [[org.hittepit.smapapi.core.result.Row Row]] en objet T
   * @return un objet T
   * @throws Exception lorsque la requête retourne plusieurs objets
   * @todo l'exception doit être plus spécifique
   */
  def unique[T](sql: String, params: List[Param[_]], mapper: Row => T): Option[T] = select(sql, params) map (mapper) match {
    case Nil => None
    case List(t) => Some(t)
    case _ => throw new Exception("More than one result") //TODO exception 
  }

  def insert[T](sql: String, params: List[Param[_]], generatedId: Column[T]): Option[T] = insert(sql,params,Some(generatedId))
  
  def insert[T](sql: String, params: List[Param[_]]): Unit = insert(sql,params,None) 
  
  private def insert[T](sql: String, params: List[Param[_]], generatedId: Option[Column[T]]): Option[T] = {
    val ps = generatedId match {
      case Some(Column(name:String, propertyType)) => connection.prepareStatement(sql, Array(name))
      case Some(Column(index:Int, propertyType)) => throw new NotImplementedError
      case _ => connection.prepareStatement(sql)
    }

    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.propertyType.setColumnValue(index + 1, param.value, ps)
      }
    }

    ps.executeUpdate()
    generatedId match {
      case Some(Column(name:String, propertyType)) =>
        val queryResult = new QueryResult(ps.getGeneratedKeys())
        queryResult.map { row => row.getColumnValue(1, propertyType) } match {
          case List(m) => Some(m)
          case List() => None
          case _ => throw new Exception("Multiple generated objects")
        }
      case Some(Column(index:Int, propertyType)) => throw new NotImplementedError
      case None => None
    }
  }
  
  def execute(sql:String, params:List[Param[_]]):Int = {
    val ps = connection.prepareStatement(sql)
    params.zipWithIndex.foreach {
      _ match {
        case (param: Param[_], index) => param.propertyType.setColumnValue(index + 1, param.value, ps)
      }
    }
    ps.executeUpdate()
  }
  
  def commit() = connection.commit()
  def rollback() = connection.rollback()
  def close() = connection.close //TODO l'état de la session doit être clair, si elle est fermée, elle ne peut plus être utilisée
}