(ns cassandra
  (:import (org.apache.cassandra.service Cassandra$Client column_t batch_mutation_t)
           (org.apache.thrift.transport TSocket)
           (org.apache.thrift.protocol TBinaryProtocol)))

(defn make-client
  [host port]
  (let [t (TSocket. host port)
        p (TBinaryProtocol. t)]
    (.open t)
    (proxy [Cassandra$Client] [p]
      (transport [] t)
      (close [] (.close t)))))

(def #^{:doc "Use this hierarchy to modify dispatch of the ->bytes multimethod."} 
     ->bytes-hierarchy (make-hierarchy))

(defn col-path
  [col-parent col]
  (str col-parent ":" col))

(defn map->column_t
  [m]
  (column_t. (:name m)
             (:value m)
             (:timestamp m)))

(defn column_t->map
  [c]
  {:name (.columnName c)
   :value (.value c)
   :timestamp (.timestamp c)})

(defn- strify
  [keyword-or-other]
  (if (keyword? keyword-or-other)
    (name keyword-or-other)
    (str keyword-or-other)))

(defmulti ->bytes 
  "Convert a type to bytes for storage in Cassandra." 
  type :hierarchy #'->bytes-hierarchy)
(defmethod ->bytes :default [x]
  (.getBytes (str x) "UTF-8"))

(defn insert-single
  ([client table key col-parent col data]
     (insert-single client table key col-parent col data (System/currentTimeMillis)))
  ([client table key col-parent col data timestamp]
     (insert-single client table key col-parent col data timestamp -1))
  ([client table key col-parent col data timestamp block-for]
     (let [col-path (col-path col-parent col)]
       (.insert client table key col-path data timestamp block-for))))

(defn insert-batch
  ([client table key cfmap]
     (insert-batch client table key cfmap -1))
  ([client table key cfmap block-for]
     (let [cfm (into {} (map (fn [[k v]] 
                               [k (map map->column_t v)])
                             cfmap))]
       (.batch_insert client (batch_mutation_t. table key cfm) block-for))))

(defn insert 
  [client table key parent colmap]
  (let [ks (keys colmap)
        vs (vals colmap)]
    (if (== 1 (count ks))
      (insert-single client table key parent (strify (first ks)) (->bytes (first vs)))
      (insert-batch client table key {(strify parent) (map (fn [k v]
                                                             {:name (strify k)
                                                              :value (->bytes v)
                                                              :timestamp (System/currentTimeMillis)})
                                                           ks vs)}))))

(defn get-column
  ([client table key col-parent col]
     (let [c (.get_column client table key (col-path col-parent col))]
       {:name (.columnName c)
        :value (.value c)
        :timestamp (.timestamp c)})))

(defn get-slice
  ([client table key col-parent]
     (get-slice client table key col-parent -1 -1))
  ([client table key col-parent start count]
     (.get_slice client table key col-parent start count)))

(defn get-slice-by-names
  [client table key col-parent cols]
  (map column_t->map 
       (.get_slice_by_names client table key col-parent cols)))

(defn fetch 
  {:arglists '([client table key parent cols])}
  [client table key parent cols]
  (let [cols->decoder (if (map? cols)
                        cols
                        (into {} (map #(vector % (fn [bytes] (String. bytes "UTF-8")))
                                      cols)))]
    (into {} (map (fn [result]
                    (let [kw-name (keyword (:name result))]
                      [kw-name ((cols->decoder kw-name) (:value result))])) 
                  (get-slice-by-names client table key parent (map strify (keys cols->decoder)))))))

(defn delete
  ([client table key parent]
     (.remove client table key parent (System/currentTimeMillis) -1))
  ([client table key parent col]
     (.remove client table key (str parent ":" (strify col)) (System/currentTimeMillis) -1)))