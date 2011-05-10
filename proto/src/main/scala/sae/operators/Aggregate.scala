package sae
package operators
import scala.collection.mutable.ListBuffer

import sae.collections._
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

/**
 * An aggregate operator receives a list of functions and a list of attributes.
 * The functions are evaluated on all tuples that coincide in all supplied attributes.
 * Each function has one (or several) attributes as its domain.
 * The list of attributes serves as a grouping key. This key is used to split the relation into
 * a relation that has only distinct combinations in the supplied attributes.
 * (Note that this grouping is a projection in terms of Codds original relational algebra)
 * The list of grouping attributes is optional.
 * If no grouping is supplied, the aggregation functions are applied on the entire relation.
 * If no function is supplied the aggregation has no effect.
 */
class Aggregation[Domain <: AnyRef, Key <: AnyRef, AggregationValue <: AnyRef, Result <: AnyRef](val source : Observable[Domain], val groupFunction : Domain => Key, aggregationFuncFactory : AggregationFunktionFactory[Domain, AggregationValue],
                                                                                                   aggragationConstructorFunc : (Key, AggregationValue) => Result)
    extends Observer[Domain] with LazyView[Result] {

    def lazy_foreach[T](f : (Result) => T){
        groups.foreach( x => f(x._2._4))
    }

    private class Count {
        private var count : Int = 0

        def inc() = { this.count += 1 }
        def dec() : Int = { this.count -= 1; this.count }

        def apply() = this.count
    }
    //TODO Evaluate cost of wrapping java.iterabel in scala iterable 
    import com.google.common.collect.HashMultiset;
    private var groups = Map[Key, (Count, HashMultiset[Domain], AggregationFunktion[Domain, AggregationValue], Result)]()

    source.addObserver(this)

    def updated(oldV : Domain, newV : Domain) {
        val oldKey = groupFunction(oldV)
        val newKey = groupFunction(newV)
        if (oldKey == newKey) {
            
            val (count, data, aggFuncs, oldResult) = groups(oldKey)
            data.remove(oldV)
            data.add(newV)
            val aggRes = aggFuncs.update(oldV, newV, data)
            val res = aggragationConstructorFunc(oldKey, aggRes)
            groups.put(oldKey , (count, data, aggFuncs, res))
            element_updated(oldResult, res)
        } else {
            removed(oldV);
            added(newV);
        }
    }

    def removed(v : Domain) {
        val key = groupFunction(v)
        val (count, data, aggFuncs, oldResult) = groups(key)
        
        if (count.dec == 0) {
            //remove a group
            groups -= key
            element_removed(oldResult)
        }else{
        data.remove(v)
        val aggRes = aggFuncs.remove(v, data)
        val res = aggragationConstructorFunc(key, aggRes)
        if (res != oldResult) {
            //some aggragation valus changed => updated event
            groups.put(key , (count, data, aggFuncs, res))
            element_updated(oldResult, res)
        }}
    }

    def added(v : Domain) {
        val key = groupFunction(v)
        if (groups.contains(key)) {
            val (count, data, aggFuncs, oldResult) = groups(key)
            data.add(v)
            count.inc
            val aggRes = aggFuncs.add(v, data)
            val res = aggragationConstructorFunc(key, aggRes)
            if (res != oldResult) {
                //some aggragation valus changed => updated event
                groups.put(key , (count, data, aggFuncs, res))
                element_updated(oldResult, res)
             
            }
        } else {
        	val c = new Count
        	c.inc
        	val data = HashMultiset.create[Domain]()
        	data.add(v)
        	val aggFuncs = aggregationFuncFactory()
        	val aggRes = aggFuncs.add(v, data)
            val res = aggragationConstructorFunc(key, aggRes)
            //groups.add(key, (c,data,aggFuncs, res))
            groups.put(key, (c,data,aggFuncs, res))
            element_added(res)
        }
    }
}



/*trait AggregationFunktionsFactory[Domain <: AnyRef, AggregationValue] {
    def apply() : AggregationFunktions[Domain, AggregationValue]
}*/
trait AggregationFunktionFactory[Domain <: AnyRef, AggregationValue] {
    def apply() : AggregationFunktion[Domain, AggregationValue]
}

/*trait AggregationFunktions[Domain <: AnyRef, AggregationValue] {
    def add(newD : Domain, data : Iterable[Domain]) : AggregationValue
    def remove(newD : Domain, data : Iterable[Domain]) : AggregationValue
    def update(oldD : Domain, newD : Domain, data : Iterable[Domain]) : AggregationValue
}*/
trait AggregationFunktion[Domain <: AnyRef, Result] {

    def add(newD : Domain, data : Iterable[Domain]) : Result
    def remove(newD : Domain, data : Iterable[Domain]) : Result
    def update(oldD : Domain, newD : Domain, data : Iterable[Domain]) : Result
}

