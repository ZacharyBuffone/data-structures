using System;
using System.Threading;

namespace Psi
{
    public class SimplePriorityQueue<T> : PriorityQueue<T>
    {
        public SimplePriorityQueue(Comparison<T> comparer) : base(comparer)
        {            
        }

        protected override bool DequeueCondition(T item)
        {
            return true;
        }
    }
    /// <summary>
    /// A generic ordered queue that sorts items based on the specified Comparer
    /// </summary>
    /// <typeparam name="T">Type of item in the list</typeparam>
    public abstract class PriorityQueue<T>
    {
        // the head of the ordered work item list is always empty
        private readonly PriorityQueueNode head = new PriorityQueueNode(0);
        private readonly PriorityQueueNode emptyHead = new PriorityQueueNode(0);
        private readonly Comparison<T> comparer;

        private ManualResetEvent empty = new ManualResetEvent(true);
        private int count;
        private int nextId;

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQueue{T}"/> class.
        /// </summary>
        /// <param name="comparer">Comparison function.</param>
        public PriorityQueue(Comparison<T> comparer)
        {
            this.comparer = comparer;
        }

        /// <summary>
        /// Gets count of items in queue.
        /// </summary>
        public int Count => this.count;

        internal WaitHandle Empty => this.empty;

        /// <summary>
        /// Try peeking at first item; returning indication of success.
        /// </summary>
        /// <param name="workitem">Work item populated if successful.</param>
        /// <returns>Indication of success.</returns>
        public bool TryPeek(out T workitem)
        {
            workitem = default(T);
            if (this.count == 0)
            {
                return false;
            }

            // lock the head and the first work item
            var previous = this.head;
            int retries = previous.Lock();
            var current = previous.Next;
            if (current == null)
            {
                return false;
            }

            current.Lock();
            workitem = current.Workitem;
            current.Release();
            previous.Release();

            return true;
        }

        /// <summary>
        /// Try to dequeue work item; returning indication of success.
        /// </summary>
        /// <param name="workitem">Work item populated if successful.</param>
        /// <param name="getAnyMatchingItem">Whether to match any item (or only first).</param>
        /// <returns>Indication of success.</returns>
        public bool TryDequeue(out T workitem, bool getAnyMatchingItem = true)
        {
            // keep looping until we either get a work item, or the list changed under us
            workitem = default(T);
            bool found = false;
            if (this.count == 0)
            {
                return false;
            }

            // as we traverse the list of nodes, we use two locks, on previous and current, to ensure consistency
            // start by taking a lock on the head first, which is immutable (not an actual work item)
            var previous = this.head;
            int retries = previous.Lock();
            var current = previous.Next;
            while (current != null)
            {
                retries += current.Lock();

                // we got the node, now see if it's ready
                if (this.DequeueCondition(current.Workitem))
                {
                    // save the work item
                    workitem = current.Workitem;
                    found = true;

                    // release the list
                    previous.Next = current.Next;

                    // add the node to the empty list
                    current.Workitem = default(T);
                    retries += this.emptyHead.Lock();
                    current.Next = this.emptyHead.Next;
                    this.emptyHead.Next = current;
                    this.emptyHead.Release();
                    current.Release();

                    break;
                }

                // keep going through the list
                previous.Release();
                previous = current;
                current = current.Next;

                if (!getAnyMatchingItem)
                {
                    break;
                }
            }

            previous.Release();

            if (found && Interlocked.Decrement(ref this.count) == 0)
            {
                this.empty.Set();
            }

            return found;
        }

        public T Dequeue()
        {
            if (TryDequeue(out var item))
            {
                return item;
            }

            return default;
        }

        /// <summary>
        /// Enqueue work item.
        /// </summary>
        /// <remarks>
        /// Enqueuing is O(n), but since we re-enqueue the oldest originating time many times as it is processed by the pipeline,
        /// the dominant operation is to enqueue at the beginning of the queue
        /// </remarks>
        /// <param name="workitem">Work item to enqueue.</param>
        public void Enqueue(T workitem)
        {
            // reset the empty signal as needed
            if (this.count == 0)
            {
                this.empty.Reset();
            }

            // take the head of the empty list
            int retries = this.emptyHead.Lock();
            var newNode = this.emptyHead.Next;
            if (newNode != null)
            {
                this.emptyHead.Next = newNode.Next;
            }
            else
            {
                newNode = new PriorityQueueNode(Interlocked.Increment(ref this.nextId));
            }

            this.emptyHead.Release();

            newNode.Workitem = workitem;

            // insert it in the right place
            retries += this.Enqueue(newNode);
  
        }

 
        /// <summary>
        /// Predicate function condition under which to dequeue.
        /// </summary>
        /// <param name="item">Candidate item.</param>
        /// <returns>Whether to dequeue.</returns>
        protected abstract bool DequeueCondition(T item);

        private int Enqueue(PriorityQueueNode node)
        {
            // we'll insert the node between "previous" and "next"
            var previous = this.head;
            int retries = previous.Lock();
            var next = this.head.Next;
            while (next != null && this.comparer(node.Workitem, next.Workitem) > 0)
            {
                retries += next.Lock();
                previous.Release();
                previous = next;
                next = previous.Next;
            }

            node.Next = previous.Next;
            previous.Next = node;

            // increment the count and signal the empty queue if needed, before releasing the previous node
            // If we didn't and this was a 0-1 transition of this.count, another thread could dequeue and go to -1,
            // we would still bring it back to 0, but we would miss signaling the empty queue.
            if (Interlocked.Increment(ref this.count) == 1)
            {
                this.empty.Reset();
            }

            previous.Release();
            return retries;
        }

#pragma warning disable SA1401 // Fields must be private
        private class PriorityQueueNode
        {
            public T Workitem;
            private readonly SynchronizationLock simpleLock;
            private PriorityQueueNode next;
            private int id;

            public PriorityQueueNode(int id)
            {
                this.id = id;
                this.simpleLock = new SynchronizationLock(this, false);
            }

            public PriorityQueueNode Next
            {
                get { return this.next; }

                set
                {
                    if (value != null && value.id == this.id)
                    {
                        throw new InvalidOperationException("A node is pointing to itself.");
                    }

                    this.next = value;
                }
            }

            public int Lock() => this.simpleLock.Lock();

            public void Release() => this.simpleLock.Release();
        }
#pragma warning restore SA1401 // Fields must be private
    }

    /// <summary>
    /// Implements a simple lock. Unlike Monitor, this class doesn't enforce thread ownership.
    /// </summary>
    public sealed class SynchronizationLock
    {
        private int counter;
        private object owner;

        public SynchronizationLock(object owner, bool locked = false)
        {
            this.owner = owner;
            this.counter = locked ? 1 : 0;
        }

        /// <summary>
        /// Prevents anybody else from locking the lock, regardless of current state (i.e. NOT exclusive).
        /// </summary>
        public void Hold()
        {
            Interlocked.Increment(ref this.counter);
        }

        /// <summary>
        /// Attempts to take exclusive hold of the lock.
        /// </summary>
        /// <returns>True if no one else was holding the lock</returns>
        public bool TryLock()
        {
            var v = Interlocked.CompareExchange(ref this.counter, 1, 0);
            return v == 0;
        }

        /// <summary>
        /// Spins until the lock is acquired, with no back-off
        /// </summary>
        /// <returns>Number of spins before the lock was acquired</returns>
        public int Lock()
        {
            SpinWait sw = default(SpinWait);
            while (!this.TryLock())
            {
                sw.SpinOnce();
            }

            return sw.Count;
        }

        /// <summary>
        /// Releases the hold on the lock.
        /// </summary>
        public void Release()
        {
            var v = Interlocked.Decrement(ref this.counter);
            if (v < 0)
            {
                throw new InvalidOperationException("The lock hold was released too many times.");
            }
        }
    }
}