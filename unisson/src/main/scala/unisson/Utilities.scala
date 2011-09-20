package unisson

import ast._
import prolog.parser.UnissonPrologParser
import prolog.utils.ISOPrologStringConversion
import queries.QueryCompiler
import sae.bytecode.model._
import de.tud.cs.st.bat.{Type, ObjectType}
import sae.bytecode.BytecodeDatabase
import java.io._

/**
 *
 * Author: Ralf Mitschke
 * Created: 05.09.11 09:59
 *
 * TODO refactor string utils and checking utils and parsing utils
 */
object Utilities
{
    private val ensembleFunctor = "ensemble"

    private val dependencyConstraintFunctors = List("incoming", "outgoing", "not_allowed", "inAndOut", "expected")

    private val parser = new UnissonPrologParser()

    /**
     * Given a delimiter (e.g. ;) returns a string in the form:
     * Ensemble;EnsembleElementCount
     */
    def ensembleToString(ensemble: Ensemble)(implicit delimiter: String, checker: ArchitectureChecker) =
    {
        ensemble.name + delimiter + checker.ensembleElements(ensemble).size
    }

    def ensembleListToString(ensembles: Seq[Ensemble]): String =
    {
        ensembles.map(_.name).reduceLeft(_ + " | " + _)
    }

    /**
     * Given a delimiter (e.g. ;) returns a string in the form:
     * Type;Kind;Source Ensembles(s);Target Ensembles(s);Violation Count
     */
    def constraintToString(constraint: DependencyConstraint)(implicit delimiter: String, checker: ArchitectureChecker) =
        constraint match {
            case IncomingConstraint(
            sources,
            target,
            kind
            ) => "incoming" + delimiter +
                    kind.designator + delimiter +
                    ensembleListToString(sources) + delimiter +
                    target.name + delimiter +
                    checker.violations(constraint).size
            case OutgoingConstraint(
            source,
            targets,
            kind
            ) => "outgoing" + delimiter +
                    kind.designator + delimiter +
                    source.name + delimiter +
                    ensembleListToString(targets) + delimiter +
                    checker.violations(constraint).size
            case NotAllowedConstraint(
            source,
            target,
            kind
            ) => "not_allowed" + delimiter +
                    kind.designator + delimiter +
                    source.name + delimiter +
                    target.name + delimiter +
                    checker.violations(constraint).size
            case ExpectedConstraint(
            source,
            target,
            kind
            ) => "expected" + delimiter +
                    kind.designator + delimiter +
                    source.name + delimiter +
                    target.name + delimiter +
                    checker.violations(constraint).size
        }

    /**
     * Converts the violation to a delimited string
     * sourceEnsemble | targetEnsemble | constraintType| violationType | sourceElement | targetElement
     */
    def violationToString(violation: Violation)(implicit delimiter: String, checker: ArchitectureChecker): String =
    {
        val data = violation match {
            case Violation(
            source,
            sourceElement,
            target,
            targetElement,
            constraint,
            kind
            ) => List(
                if (source == None) {
                    ensembleListToString(ensmblesForElement(sourceElement))
                } else {
                    source.get.name
                },
                if (target == None) {
                    ensembleListToString(ensmblesForElement(targetElement))
                } else {
                    target.get.name
                },
                constraintType(constraint),
                kind,
                elementToString(sourceElement),
                elementToString(targetElement)
            )
        }
        data.foldRight("")(_ + delimiter + _)
    }


    def constraintType(constraint: DependencyConstraint): String =
    {
        constraint match {
            case NotAllowedConstraint(_, _, kind) => "not_allowed(" + kind + ")"
            case ExpectedConstraint(_, _, kind) => "expected(" + kind + ")"
            case IncomingConstraint(_, _, kind) => "incoming(" + kind + ")"
            case OutgoingConstraint(_, _, kind) => "outgoing(" + kind + ")"
        }
    }

    def elementToString[T](elem: SourceElement[T])(implicit delimiter: String): String =
    {
        elem match {
            case SourceElement(ObjectType(name)) => "class" + delimiter + name
            case SourceElement(t: Type) => "type" + delimiter + t.toJava
            case SourceElement(
            Method(
            decl,
            name,
            params,
            ret
            )
            ) => "method" + delimiter + decl.toJava + "." + name + "(" + (params.foldLeft("")(_ + ", " + _.toJava)).drop(
                2
            ) + ")" + ": " + ret.toJava

            case SourceElement(
            Field(
            decl,
            name,
            typ
            )
            ) => "field" + delimiter + decl.toJava + "." + name + ": " + typ.toJava
        }
    }

    def ensmblesForElement(sourceElement: SourceElement[AnyRef])(implicit checker: ArchitectureChecker): Seq[Ensemble] =
    {

        val ensembles = checker.getEnsembles.filter(
                (ensemble: Ensemble) => {
                val elements = checker.ensembleElements(ensemble);
                if (!ensemble.name.startsWith("@") && elements.contains(sourceElement)) {
                    true
                }
                else {
                    false
                }
            }
        )

        if (!ensembles.isEmpty) {
            ensembles.toList
        }
        else {
            Set(CloudEnsemble).toList
        }
    }


    private val classPattern = """.*\.class""".r

    private val jarPattern = """.*\.jar""".r

    def checkArchitectures(sadFiles: Array[String], codeLocations: Array[String]): ArchitectureChecker =
    {
        val database = new BytecodeDatabase
        val checker = new ArchitectureChecker(database)
        val compiler = new QueryCompiler(checker)

        sadFiles.foreach(
                (sadFile: String) => {
                val plFile = sadFile + ".pl"
                println("reading architecture from " + plFile)
                compiler.addAll(
                    readSadFile(
                        fileNameAsStream(plFile)
                    )
                )
            }
        )
        compiler.finishOutgoing()


        codeLocations.map(
                (loc: String) => loc match {
                case classPattern() => {
                    println("reading bytecode from " + loc)
                    database.transformerForClassfileStream(fileNameAsStream(loc)).processAllFacts()
                }
                case jarPattern() => {
                    println("reading bytecode from " + loc)
                    database.transformerForArchiveStream(fileNameAsStream(loc)).processAllFacts()
                }
                case _ => println("unrecognized code location type : " + loc)
            }
        )

        checker
    }


    def readSadFile(stream: InputStream): Seq[UnissonDefinition] =
    {
        val in = new BufferedReader(new InputStreamReader(stream))
        var result: Seq[UnissonDefinition] = Nil
        while (in.ready) {

            val s = in.readLine();
            if (s.trim().length() > 0 && !s.trim().startsWith("%") && !s.trim().startsWith(":-")) {
                result = result :+ readPrologLine(s)
            }
        }

        ResolveAST(result)
    }

    def resourceAsStream(name: String) =
    {
        this.getClass.getClassLoader.getResourceAsStream(name)
    }

    def fileNameAsStream(name: String) =
    {
        val file = new File(name)
        new FileInputStream(file)
    }


    def readPrologLine(s: String): UnissonDefinition =
    {
        // TODO move functor recognition to the parser
        val functor = ISOPrologStringConversion.getFunctor(s)
        if (functor == ensembleFunctor) {
            return readEnsemble(s)
        }
        else if (dependencyConstraintFunctors.contains(functor)) {
            return readDependencyConstraint(s)
        }
        throw new IllegalArgumentException("can not parse the following string: " + s)
    }

    def readEnsemble(s: String): UnresolvedEnsemble =
    {
        val result = parser.parseAll(parser.ensemble, s)
        result match {
            case parser.Failure(msg, next) => {
                println("unable to parse ensemble:")
                println(msg)
                println(next.pos.longString)
                System.exit(-1)
                null
            }
            case parser.Success(ensemble, _) => ensemble
        }
    }

    def readDependencyConstraint(s: String): DependencyConstraintEdge =
    {
        val result = parser.parseAll(parser.dependencyConstraint, s)
        result match {
            case parser.Failure(msg, next) => {
                println("unable to parse dependency:")
                println(msg)
                println(next.pos.longString)
                System.exit(-1)
                null
            }
            case parser.Success(dependency, _) => dependency
        }
    }


}