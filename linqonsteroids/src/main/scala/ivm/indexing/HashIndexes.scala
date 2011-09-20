package ivm
package indexing

import scala.collection.mutable.HashMap
import expressiontree.{FuncExp, QueryReifier}


/*
 * These indexes generalize path indexes as described in the following paper:
 * Bertino, E., Kim, W.: Indexing Techniques for Queries on Nested Objects. IEEE Trans. Knowl. Data Eng.(1989) 196-214
 *
 * These indexes are more general, because the paths need not consist of attribute names
 * but are the result of arbitrary function computations
 *
 * Note: It would also be useful to have a generalized form of the multi indexes as described in the paper above.
 * Lookup is slower but maintenance is cheaper. Furthermore, they presumably work well together with
 * sharing in the object graph, i.e., index replication can be avoided.
 */

class HashIndex[T,S](it: QueryReifier[T], f: FuncExp[T,S] ) extends HashMap[S,Traversable[T]] with Index[S,Traversable[T]] {
  this ++= it.exec().groupBy(f.interpret())
}

class HashIndex2[T1,T2,S](it: QueryReifier[T1],
                                     p1: FuncExp[T1,QueryReifier[T2]],
                                     f: FuncExp[(T1,T2),S]) extends HashMap[S,Traversable[(T1,T2)]]
                                                            with Index[S,Traversable[(T1,T2)]] {
  this ++= it.exec().
    flatMap( (x) => p1.interpret()(x).exec().map( (y) => (x,y))).
    groupBy( (p) => ( f.interpret()(p)))
}


class HashIndex3[T1,T2,T3,S](it: QueryReifier[T1],
                                     p1: FuncExp[T1,QueryReifier[T2]],
                                     p2: FuncExp[T2,QueryReifier[T3]],
                                     f: FuncExp[(T1,T2,T3),S])
                                    extends HashMap[S,Traversable[(T1,T2,T3)]]
                                    with Index[S,Traversable[(T1,T2,T3)]]{
  this ++=
    it.exec().
      flatMap( (x) => p1.interpret()(x).exec().map( (y) => (x,y))).
      flatMap( (z) => p2.interpret()(z._2).exec().map( (y) => (z._1,z._2,y))).
      groupBy( (p) => ( f.interpret()(p)))
}

class HashIndex4[T1,T2,T3,T4,S](it: QueryReifier[T1],
                                     p1: FuncExp[T1,QueryReifier[T2]],
                                     p2: FuncExp[T2,QueryReifier[T3]],
                                     p3: FuncExp[T3,QueryReifier[T4]],
                                     f: FuncExp[(T1,T2,T3,T4),S])
    extends HashMap[S,Traversable[(T1,T2,T3,T4)]]
    with Index[S,Traversable[(T1,T2,T3,T4)]] {
  this ++=
    it.exec().
      flatMap( (x) =>  p1.interpret()(x).exec().map( (y) => (x,y))).
      flatMap( (z) => p2.interpret()(z._2).exec().map( (y) => (z._1,z._2,y))).
      flatMap( (z) => p3.interpret()(z._3).exec().map( (y) => (z._1,z._2,z._3,y))).
      groupBy( (p) => ( f.interpret()(p)))
}

// KO: hopefully all code above is subsumed by PathIndex. I leave it there until path index optimization is implemented.
class PathIndex[T1, P, S, +T](it: QueryReifier[T1], path: Path[(T1, P), T], fGroup: FuncExp[(T1, P), S])
   extends HashMap[S, Traversable[(T1, P)]]
   with Index[S, Traversable[(T1, P)]] {
  private def traversePath[T, R, Q](c: Traversable[T], p: Path[(T, R), Q]): Traversable[(T, R)] = {
    p match {
      case EmptyPath() =>
        c map (x => (x, ()))
      // Wow, Scala is smart enough to typecheck the second branch!
      case ConsPath(currF, currPath) =>
        c.flatMap(x => traversePath(currF.interpret()(x).exec(), currPath) map (y => (x, y)))
    }
  }

  this ++= traversePath(it.exec(), path).groupBy(p => fGroup.interpret()(p))
}

import expressiontree.Lifting._
import expressiontree.Const

/**
 * As an experiment and example of using PathIndex, define (partial) adapters between the two kinds of
 * interfaces given above. These adaptors are partial because they accept the same parameters, but
 * extend Map and Index with different parameter types - nested tuples instead of flat ones.
 * XXX: many of these encodings are not adequate, because they rely on interpret(). Moreover, extracting f from FuncExp
 * is not elegant; FuncExp should have an apply method constructing App nodes.
 */

class PathIndex1[T, S](it: QueryReifier[T], f: FuncExp[T, S])
  extends PathIndex[T, Unit, S, T](
    it,
    EmptyPath(),
    FuncExp(x => f.f(x._1)))

class PathIndex1Unit[T, S](it: QueryReifier[T], path: Path[(T, Unit), T], fGroup: FuncExp[(T, Unit), S])
  extends PathIndex(it, EmptyPath[T](), fGroup)

class PathIndex2[T1, T2, S](it: QueryReifier[T1],
                                     p1: FuncExp[T1, QueryReifier[T2]],
                                     f: FuncExp[(T1, T2), S])
  extends PathIndex[T1, (T2, Unit), S, T2](
    it,
    ConsPath(p1, EmptyPath()),
    FuncExp(x => f.f((x._1, x._2._1))))

class PathIndex3[T1, T2, T3, S](it: QueryReifier[T1],
                                     p1: FuncExp[T1, QueryReifier[T2]],
                                     p2: FuncExp[T2, QueryReifier[T3]],
                                     f: FuncExp[(T1, T2, T3), S])
  extends PathIndex[T1, (T2, (T3, Unit)), S, T3](
    it,
    ConsPath(p1, ConsPath(p2, EmptyPath())),
    //_1 and interpret() commute.
    FuncExp(x => Const(f.interpret()((x.interpret()._1, x.interpret()._2._1, x.interpret()._2._2._1)))))
    //The correct term is (probably) this:
    //FuncExp(x => f.f((x._1, x._2._1, x._2._2._1))))
    //But it requires lifting for triples.

class PathIndex4[T1, T2, T3, T4, S](it: QueryReifier[T1],
                                     p1: FuncExp[T1, QueryReifier[T2]],
                                     p2: FuncExp[T2, QueryReifier[T3]],
                                     p3: FuncExp[T3, QueryReifier[T4]],
                                     f: FuncExp[(T1, T2, T3, T4), S])
  extends PathIndex[T1, (T2, (T3, (T4, Unit))), S, T4](
    it,
    ConsPath(p1, ConsPath(p2, ConsPath(p3, EmptyPath()))),
    FuncExp(x => Const(f.interpret()((
      x.interpret()._1,
      x.interpret()._2._1,
      x.interpret()._2._2._1,
      x.interpret()._2._2._2._1)))))
    //The correct term is (probably) this:
    /*FuncExp(x => f.f((
      x._1,
      x._2._1,
      x._2._2._1,
      x._2._2._2._1))))*/
    //But it requires lifting for quadruples.