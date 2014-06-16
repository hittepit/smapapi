package org.hittepit.smapapi.core.result

import org.hittepit.smapapi.core.PropertyType
import java.sql.ResultSet

/**
 * Classe qui encapsule un ResultSet. Elle permet de mapper les lignes du ResultSet vers des objets.
 * @constructor Crée un objet QueryResult
 * @param resultSet le ResultSet encapsulé
 */
class QueryResult(val resultSet: ResultSet) {
  /**
   * Transforme un ResultSet en objets de type T
   * @param mapper fonction qui transforme une [[Row]] dans l'objet cible de type T
   */
  def map[T](mapper: Row => T): List[T] = {
    def innerMap(acc: List[T]): List[T] = nextRow() match {
      case Some(row) => innerMap(acc ::: List(mapper(row)))
      case None => acc
    }

    innerMap(Nil)
  }
  
  /**
   * Génère une [[Row]] correspondant à la ligne suivante du ResultSet, s'il y en a.
   * @return Some(row) où row elle la [[Row]] courant, None s'il n'y a plus de row.
   */
  def nextRow():Option[Row] = if(resultSet.next) Some(new Row(resultSet)) else None
}

/**
 * Classe (uniquement) générée par [[QueryResult]] pour représenter une Row, encapsulant un ResultSet.
 * @constructor Crée un objet row
 * @param resultSet le ResultSet pointant sur la bonne row. C'est le QueryResult qui se charge de faire pointer le resultSet sur la row devant être récupérée.
 */
class Row private[result](resultSet: ResultSet) {
  /**
   * Retourne la valeur d'une colonne dans un ResultSet à partir de son index
   * @param index index de la colonne
   * @param PropertyType le type de la valeur de la Colonne
   * @return la valeur de la colonne
   */
  def getColumnValue[T](index: Int, PropertyType: PropertyType[T]) = PropertyType.getColumnValue(resultSet, Right(index))
  
  /**
   * Retourne la valeur d'une colonne dans un ResultSet à partir de son nom
   * @param columnName nom de la colonne
   * @param PropertyType le type de la valeur de la Colonne
   * @return la valeur de la colonne
   */
  def getColumnValue[T](columnName: String, PropertyType: PropertyType[T]) = PropertyType.getColumnValue(resultSet, Left(columnName))
}