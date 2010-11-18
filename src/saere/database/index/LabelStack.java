package saere.database.index;

import scala.actors.threadpool.Arrays;

/**
 * Mimics a (read-only) stack by wrapping an array. It's actually 
 * nothing more than a state, i.e., an integer position, of an array.<br />
 * <br />
 * <b>Note that a push operation is not supported.</b>
 * 
 * @author David Sullivan
 * @version 0.7, 10/17/2010
 */
public final class LabelStack {
	
	private Label[] labels;
	private int position;
	
	/**
	 * Creates a new {@link LabelStack} with the specified array.
	 * 
	 * @param labels The underlying array for this {@link LabelStack}.
	 */
	public LabelStack(Label[] labels) {
		this(labels, 0);
	}
	
	/**
	 * Creates a new {@link LabelStack} with its first element at the specified 
	 * <tt>position</tt> index of the specified array.
	 * 
	 * @param labels The underlying array for this {@link LabelStack}.
	 * @param position The first element's index the underlying array.
	 */
	private LabelStack(Label[] labels, int position) {
		this.labels = labels;
		this.position = position;
	}
	
	/**
	 * Retrieves the first element and moves the position forward. That 
	 * is, multiple calls will always return a new first element until no 
	 * element is left.<br/>
	 * <br/>
	 * <b>Warning: Doesn't differ between an out of bound call and a null 
	 * element.</b>
	 * 
	 * @return The first element.
	 */
	public Label pop() {
		Label value = peek();
		position++;
		return value;
	}
	
	public Label pop(int number) {
		assert number > 0 && position + number < labels.length : "Illegal number " + number;
		
		Label value = pop();
		if (number > 1) {
			if (number > 2)
				position += (number - 2); // jump
			value = pop();
		}
		
		return value;
	}
	
	/**
	 * Retrieves the first element but does not move the position forward. That 
	 * is, multiple calls will always return the same first element.<br/>
	 * <br/>
	 * <b>Warning: Doesn't differ between an out of bound call and a null 
	 * element.</b>
	 * 
	 * @return The first element.
	 */
	public Label peek() {
		if (position < labels.length) {
			assert labels[position] != null : "Term stack element is null";
			return labels[position];
		} else {
			return null;
		}
	}
	
	public Label peek(int number) {
		assert number > 0 && position + number < labels.length : "illegal number";
		
		if (number == 1) {
			return peek();
		} else {
			int peekPosition = position + number;
			if (peekPosition < labels.length) {
				return labels[peekPosition - 1];
			} else {
				return null;
			}
		}
	}
	
	/**
	 * Moves position one index back, that is, &quot;unpops&quot; what was 
	 * popped last.
	 */
	public void back() {
		position = (position > 0) ? (position - 1) : 0;
	}
	
	public void back(int number) {
		position = (position > (number - 1)) ? (position - number) : 0;
	}
	
	/**
	 * Returns the (current) size of the stack, i.e., how many elements are 
	 * left in the stack.
	 * 
	 * @return The size of the stack.
	 */
	public int size() {
		int size = labels.length - position;
		return size > 0 ? size : 0;
	}
	
	/**
	 * The length of the underlying array.
	 * 
	 * @return The length of the underlying array.
	 * @see #size()
	 */
	public int length() {
		return labels.length;
	}
	
	public Label[] array() {
		return labels;
	}
	
	public int position() {
		return position;
	}
	
	// XXX Only for debugging!
	@Override
	public String toString() {
		if (size() > 0) {
			Label[] ls = new Label[size()];
			System.arraycopy(labels, position, ls, 0, size());
			return Arrays.toString(ls);
		} else {
			return "[]";
		}
	}
}