package org.hittepit.smapapi.core.result

import org.hittepit.smapapi.core.SqlType
import java.sql.ResultSet

/**
 * Classe qui encapsule un ResultSet. Elle permet de mapper les lignes du ResultSet vers des objets.
 * @constructor Crée un objet QueryResult
 * @param resultSet le ResultSet encapsulé
 */
class QueryResult(val resultSet: ResultSet) {
  /**
   * Transforme un ResultSet en objets de type T
   * @param mapper fonction
   */
  def map[T](mapper: Row => T): List[T] = {
    def innerMap(acc: List[T]): List[T] = if (resultSet.next) {
      innerMap(acc ::: List(mapper(new Row(resultSet))))
    } else {
      acc
    }

    innerMap(Nil)
  }
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
   * @param sqlType le type sql de la Colonne
   * @return la valeur de la colonne
   */
  def getColumnValue[T](index: Int, sqlType: SqlType[T]) = sqlType.getColumnValue(resultSet, Right(index))
  
  /**
   * Retourne la valeur d'une colonne dans un ResultSet à partir de son nom
   * @param columnName nom de la colonne
   * @param sqlType le type sql de la Colonne
   * @return la valeur de la colonne
   */
  def getColumnValue[T](columnName: String, sqlType: SqlType[T]) = sqlType.getColumnValue(resultSet, Left(columnName))
}