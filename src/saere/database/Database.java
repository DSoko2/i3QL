package saere.database;

import java.util.Iterator;
import java.util.List;

import saere.Atom;
import saere.StringAtom;
import saere.Term;
import saere.Variable;

/**
 * Abstract base class for the {@link ListDatabase} and {@link TrieDatabase}.
 * 
 * @author David Sullivan
 * @version 0.2, 9/21/2010
 */
public abstract class Database {
	
	/**
	 * Adds a term (fact) to the database.
	 * 
	 * @param fact The term to add.
	 */
	public abstract void add(Term fact);
	
	/**
	 * Fills the database with the facts of the {@link Factbase}.
	 */
	public void fill() {
		List<Term> terms = Factbase.getInstance().getFacts();
		for (Term term : terms) {
			add(term);
		}
		fillProcessComplete();
	}
	
	/**
	 * This method is called at the end of {@link #fill()} and may be used to 
	 * do some data reorganization after all facts of the {@link Factbase} are 
	 * inserted. This method does nothing by default.
	 */
	protected abstract void fillProcessComplete();

	/**
	 * Empties the whole database.
	 */
	public abstract void drop();

	/**
	 * Gets an iterator for all facts.
	 * 
	 * @return An iterator for all facts.
	 */
	public abstract Iterator<Term> getFacts();
	
	/**
	 * Gets an iterator for facts of a predicate with the specified functor.
	 * 
	 * @param functor The functor of the predicate.
	 * @return An iterator for the predicate facts.
	 */
	public abstract Iterator<Term> getFacts(StringAtom functor);
	
	/**
	 * Gets an iterator for the candidate set that was composed by the 
	 * {@link Database} with regards to the query expressed by the specified 
	 * {@link Term}<tt>[]</tt> parameter.
	 * 
	 * @param terms An array of {@link Term}s ({@link Atom}s and free {@link Variable}s) that expresses a query.
	 * @return An iterator for the candidates.
	 */
	public abstract Iterator<Term> getCandidates(Term[] terms);
}
