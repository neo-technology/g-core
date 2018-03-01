package parser.exceptions

import api.exceptions.GcoreException

abstract class ParserException(message: String) extends GcoreException(message)

case class LanguageLoadException(message: String) extends ParserException(message)

case class QueryParseException(message: String) extends ParserException(message)
