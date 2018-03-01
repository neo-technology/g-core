package parser.utils

import java.net.URL

import org.apache.commons.vfs2.FileObject
import org.metaborg.core.language.{ILanguageComponent, ILanguageDiscoveryRequest, ILanguageImpl, LanguageUtils}
import org.metaborg.spoofax.core.Spoofax
import org.metaborg.spoofax.core.unit.{ISpoofaxInputUnit, ISpoofaxParseUnit}
import org.spoofax.interpreter.terms.IStrategoTerm
import parser.exceptions.{LanguageLoadException, QueryParseException}

/** Utility object to work with the [[Spoofax]] parser and the G-CORE language component. */
object GcoreLang {

  /** File name of the G-CORE language component. */
  val gcoreGrammarSpec: String = "gcore-spoofax-0.1.0-SNAPSHOT.spoofax-language"

  val spoofax: Spoofax = new Spoofax()

  /**
    * The G-CORE language component we can use once [[Spoofax]] has loaded the grammar specification
    * from [[gcoreGrammarSpec]].
    */
  val gcoreLang: ILanguageImpl = {
    val gcoreUrl: URL = getClass.getClassLoader.getResource(gcoreGrammarSpec)
    val gcoreLocation: FileObject = spoofax.resourceService.resolve("zip:" + gcoreUrl + "!/")
    val requests: java.lang.Iterable[ILanguageDiscoveryRequest] =
      spoofax.languageDiscoveryService.request(gcoreLocation)
    val components: java.lang.Iterable[ILanguageComponent] =
      spoofax.languageDiscoveryService.discover(requests)
    val implementations: java.util.Set[ILanguageImpl] = LanguageUtils.toImpls(components)
    val lang = LanguageUtils.active(implementations)
    if (lang == null)
      throw LanguageLoadException(s"No language implementation was found at $gcoreGrammarSpec")
    lang
  }

  /** Parses a G-CORE query. */
  def parseQuery(query: String): IStrategoTerm = {
    val input: ISpoofaxInputUnit = spoofax.unitService.inputUnit(query, gcoreLang, null)
    val output: ISpoofaxParseUnit = spoofax.syntaxService.parse(input)
    if (!output.valid())
      throw QueryParseException(s"Could not parse query $query")
    output.ast()
  }
}
