package unisson.model

import unisson.query.compiler.{BaseQueryCompiler, CachingQueryCompiler}
import unisson.query.parser.QueryParser
import de.tud.cs.st.vespucci.model.IEnsemble
import unisson.query.code_model.SourceElement
import sae.{LazyView, Observer}
import sae.bytecode.Database
import de.tud.cs.st.vespucci.interfaces.ICodeElement
import sae.collections.Table
import unisson.query.UnissonQuery

/**
 *
 * Author: Ralf Mitschke
 * Date: 25.06.12
 * Time: 12:58
 *
 */
class CompiledEnsembleElementsView(bc: Database,
                                   ensembleQueries: LazyView[(IEnsemble,UnissonQuery)])
        extends LazyView[(IEnsemble, ICodeElement)]
{

    private val queryCompiler = new CachingQueryCompiler(new BaseQueryCompiler(bc))

    initialized = true

    def lazyInitialize() {
        // should never be used
        throw new UnsupportedOperationException("lazyInitialize not supported for global_ensemble_elements")
    }

    def lazy_foreach[T](f: ((IEnsemble, ICodeElement)) => T) {
        ensembleQueries.lazy_foreach((entry:(IEnsemble,UnissonQuery)) =>
                queryCompiler.compile(entry._2).lazy_foreach[Unit](
                    (e:ICodeElement) => f((entry._1, e))
                )
        )
    }


    private var elementObservers: Map[IEnsemble, CompiledViewObserver] = Map.empty


    private def addCompiledQueryView(v:IEnsemble, view : LazyView[SourceElement[AnyRef]])
    {
        view.lazy_foreach(
            (e: SourceElement[AnyRef]) => element_added((v, e))
        )
        val oo = new CompiledViewObserver(v)
        view.addObserver(oo)
        elementObservers += {
            v -> oo
        }
    }

    private def removeCompiledQueryView(v:IEnsemble, view : LazyView[SourceElement[AnyRef]])
    {
        view.lazy_foreach(
            (e: SourceElement[AnyRef]) => element_removed((v, e))
        )
        // dispose of obsolete observers
        view.removeObserver(elementObservers(v))
        elementObservers -= v
    }

    private val queryObservingCompiler = new Observer[(IEnsemble,UnissonQuery)] {


        def added(v: (IEnsemble, UnissonQuery)) {
            val compiledQuery = queryCompiler.compile(v._2)
            addCompiledQueryView(v._1, compiledQuery)
        }

        def removed(v: (IEnsemble, UnissonQuery)) {
            val compiledQuery = queryCompiler.compile(v._2)
            removeCompiledQueryView(v._1, compiledQuery)
            // dispose of obsolete views
            queryCompiler.dispose(v._2)
        }

        def updated(oldV: (IEnsemble, UnissonQuery), newV: (IEnsemble, UnissonQuery)) {
            removed(oldV)
            added(newV)
        }
    }

    ensembleQueries.addObserver(queryObservingCompiler)

    /**
     * This observer adds entries from a compiled view of source code elements.
     * Since the ensemble is not contained as an information in the compiled view (i.e., they are only a set of code elements)
     * there is one observer per ensemble.
     */
    class CompiledViewObserver(val ensemble: IEnsemble) extends Observer[SourceElement[AnyRef]]
    {
        def updated(oldV: SourceElement[AnyRef], newV: SourceElement[AnyRef]) {
            element_updated((ensemble, oldV), (ensemble, newV))
        }

        def removed(v: SourceElement[AnyRef]) {
            element_removed((ensemble, v))
        }

        def added(v: SourceElement[AnyRef]) {
            element_added((ensemble, v))
        }
    }

}