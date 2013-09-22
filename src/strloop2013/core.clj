(ns strloop2013.core)

;; our goal is a function with a similar interface to reduce
;; that executes in parallel and can accept an additional set
;; of transformations to perform along with the reducing operation
;; Since this is not an interactive session, I'm going to post-fix
;; functions with an increment number (-XX) to show each step of the refactoring
;; or refinement process

;; we need a collection first of all
(def coll (into [] (range 100)))

;; we are going to call our special reduce "preduce", which stands for
;; parallel reduce and we can invoke like reduce: (preduce + 0 coll)

(defn preduce-01 [g seed coll]
  (reduce g seed coll))

(preduce-01 + 0 coll)

;; that's working as expected and returning 4950
;; if we want to make this parallel we can just divide and 
;; conquer the collection and send it to workers when the size
;; is small enough

(defn preduce-02 [g seed coll]
  (let [split (quot (count coll) 2)
        c1 (subvec coll 0 split)
        c2 (subvec coll split (count coll))]
    (if (<= (count coll) 50)
      (reduce g seed coll)
      ;; hinc sunt dragones
      (g (preduce-02 g seed c1) (preduce-02 g seed c2)))))

(preduce-02 + 0 coll)

;; this is just an example, it's not parallel for real. Parallelization
;; requires the two subcollections to be processed by separate workers, it just
;; shows the principle. If you look at the reducers source file in Clojure core
;; you'll see how fork-join can be used to spawn the computation across multiple
;; threads. So from now on, consider preduce-XX as if it was doing real 
;; parallelism.

;; Cool. This is parallel but is just a reduce operation. We want to be able to
;; plugin any number of additional transformations, like for example incrementing
;; the number of the collection or filtering them by even numbers like so:

(reduce + (filter even? (map inc coll)))

;; which returns 2550. Now we want something like this to be potentially
;; parallel, that is executing the additional transformations while also
;; executing the reducing operation.

;; We could create our own "+" operation that can take care of any additional
;; transformation like so:

(defn plus-01 [a b]
  (+ a b))

(preduce-02 plus-01 0 coll)

;; which of course works as expected. Now we can do additional stuff over
;; that "b" operand, like an increment:

(defn plus-02 [a b]
  (+ a (inc b)))

(preduce-02 plus-02 0 coll)

;; yay, works as expected. But wait a minute, why 5051 when it was 5050?
;; we had to use a "combining" function in our preduce to sum up together
;; results coming from different threads. That combining function is assumed
;; to be "plus-02" that is different from the previous "+" we were using because
;; it is always incrementing the second operand. So the very last line of
;; preduce-02 after we are done is (plus-02 1225 1225) which adds that additional
;; one into the mix. We can fix it by passing a combining function to preduce.

(defn preduce-03 [g comb seed coll]
  (let [split (quot (count coll) 2)
        c1 (subvec coll 0 split)
        c2 (subvec coll split (count coll))]
    (if (<= (count coll) 50)
      (reduce g seed coll)
      ;; hinc sunt dragones
      (comb (preduce-03 g comb seed c1) (preduce-03 g comb seed c2)))))

(preduce-03 plus-02 + 0 coll)

;; back to 5050. Good. So are we happy now? Well, we've got a parallel reduce with
;; optional composition of an inc function, but we have to definitely make that generic.

(defn plus-03 [f]
  (fn [a b] (+ a (f b))))

(preduce-03 (plus-03 inc) + 0 coll)

;; here we added the possibility to decide what transforming function we want. Now 
;; we can do the same with the reducing function.

(defn plus-04 [f]
  (fn [g] (fn [a b] (g a (f b)))))

(preduce-03 ((plus-04 inc) +) + 0 coll)

;; and now "plus" isn't a good name, since that function is accepting
;; a generic associative function and a generic transformation. A better name would be:

(defn reducing-transformer [f]
  (fn [g] (fn [a b] (g a (f b)))))

(preduce-03 ((reducing-transformer inc) +) + 0 coll)

;; Hey, we can do similar things for a filtering semantic. What is filter? It is something
;; that if a predicate on the value of the colletion is true we reduce. If it's false, we
;; don't do anything!

(defn reducing-filter [p]
  (fn [g] (fn [a b]
            (if (p b) (g a b) a))))

(preduce-03 ((reducing-filter even?) +) + 0 coll)

;; okay, are we happy yet? Well, we lost our goal of having a similar
;; interface to reduce. I would really like to read something like:

(preduce-03 + + 0 (vec (map inc coll)))

;; but what's wrong with that? The map operation is outside parallelization, it happens
;; before we enter our divide and conquer stuff. It turns out that we can achieve that result
;; if we delegate the transformation to the collection during the reduce. Luckily for us
;; reduce in clojure.core is implemented as a protocol. So we can reify the collection
;; so that when reduce is invoked on it, we intercept the call and we do whatever we want.
;; So, this is a function that given a reducing-transformer or filter or anything else, 
;; applies it to the reducing operation when invoked on the collection we pass in as an
;; argument

(defn reducer-01 [fx coll]
  (reify clojure.core.protocols/CollReduce
    (coll-reduce [_ f]
      (clojure.core.protocols/coll-reduce coll (fx f) (f)))
    (coll-reduce [_ f seed]
      (clojure.core.protocols/coll-reduce coll (fx f) seed))))

;; Try to execute the following line to see the exception, but then
;; it needs to be stay commented out.
;; (preduce-03 + + 0 (reducer-01 (reducing-transformer inc) coll))

;; WAT? Unsupported operation exception! That's because we need an additional step.
;; our preduce-03 doesn't know anything about "reifyied" collection. We are passing in
;; a reify type (coll) and we are invoking (count coll) and count is not supported for the
;; tyoe "reify". We need to make sure that, when preduce-03 is invoked on a reified collection
;; we instruct the collection about what should happen. Is very similar to what we did with
;; the CollReduce protocol. But we need a new protocol, so our collection can reify it and
;; instruct about what to do.

(defprotocol CollParallel
  (coll-parallel [coll reducef combinef seed]))

(defn reducer-02 [fx coll]
  (reify 
    clojure.core.protocols/CollReduce
    (coll-reduce [_ f]
      (clojure.core.protocols/coll-reduce coll (fx f) (f)))
    (coll-reduce [_ f seed]
      (clojure.core.protocols/coll-reduce coll (fx f) seed))
    CollParallel
    (coll-parallel [_ reducef combinef seed]
      (preduce-03 (fx reducef) combinef seed coll))))

(coll-parallel (reducer-02 (reducing-transformer inc) coll) + + 0)

;; We got the expected result, 5050. Still a few problems with readability.
;; With the goal of reading it similarly to
;; (reduce + 0 (map inc coll))
;; we could just extract a new function

(defn fold-01 [redf comf seed coll]
  (coll-parallel (reducer-02 (reducing-transformer inc) coll) redf comf 0))

(fold-01 + + 0 coll)

;; and in general, extract the way we do the transformation, so it is explicit
;; when calling fold-01

(defn rmap [f coll]
  (reducer-02 (reducing-transformer f) coll))

(defn fold-02 [redf comf seed coll]
  (coll-parallel coll redf comf 0))

(fold-02 + + 0 (rmap inc coll))

;; and finally remove all the postfix numbers for clarity:

(defn fold [redf comf seed coll]
  (coll-parallel coll redf comf 0))

(fold + + 0 (rmap inc coll))

;; Goal achieved. Now we are happy. 
;; Let's look at how we could compose
;; inc with even? and in general compose other transforming operation
;; over to the collection.

;; rmap for filtering looks like

(defn rfilter [p coll]
  (reducer-02 (reducing-filter p) coll))

(defn tr [p f coll]
  (reducer-02 (comp (reducing-filter p) (reducing-transformer f)) coll))

(fold + + 0 (tr even? inc coll))

;; but this takes us back to a different API when we would like to see something more like
;; (fold + + 0 (rfilter even? (rmap inc coll)))
;; to do that we need some more magic, but this goes beyond the scope of our talk.
;; The magic can be found in the macros to alter the arity or r/map r/filter and friends
;; called defcurried that can be found in clojure.core.reducers.clj

(str "the end")
