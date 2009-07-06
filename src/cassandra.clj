(ns cassandra
  (:refer-clojure :exclude [get remove])
  (:require [cassandra.thrift :as thrift])
  (:use [clojure.contrib.json.read :only [read-json-string]] 
        [clojure.contrib.json.write :only [json-str]] 
        cassandra.util
        cassandra.thrift)
  (:import (org.apache.cassandra.service Cassandra$Client column_t batch_mutation_t)
           (org.apache.thrift.transport TSocket)
           (org.apache.thrift.protocol TBinaryProtocol)))

(declare make-table
         family-type-and-single-or-batch)

(defn make-client
  "Create a client map containing relevant bits for interfacing with Cassandra.
Require a host and port, the caller may include and options map to indicate an :encoder 
and :decoder for serializing and reconstructing stored data."
  [host port & opts]
  (let [opts (first opts)
        encoder (:encoder opts)
        decoder (:decoder opts)
        t (TSocket. host port)
        p (TBinaryProtocol. t)
        c (do (.open t)
              (proxy [Cassandra$Client] [p]
                (transport [] t)
                (close [] (.close t))))]
    {:thrift-client c
     :tables (into {} (map (fn [t] [(:name t) t]) 
                           (map #(make-table %1 %2 %3 %4) 
                                (repeat c) 
                                (.getStringListProperty c "tables")
                                (repeat (fn [data] (-> (json-str data)
                                                       (.getBytes "UTF-8"))))
                                (repeat (fn [bytes] (-> (String. bytes "UTF-8") 
                                                        read-json-string))))))}))

(defn- make-table
  "Make a table object that references an existing Cassandra table."
  [client table-name default-encoder default-decoder]
  (let [schema (parse-table-schema (.describeTable client table-name))]
    {:type ::table
     :client client
     :name (keyword table-name)
     :families schema
     :encoder default-encoder
     :decoder default-decoder}))

(defn family-type-and-single-or-batch
  [t k f c-or-cs & r] 
  [(-> t :families f :type) 
   (if (and (coll? c-or-cs)
            (> (count c-or-cs) 1))
     ::batch
     ::single)])

(defmulti put family-type-and-single-or-batch)
(defmethod put [::standard ::single]
  [table key family-kw col-val & opts]
  (insert-single table key family-kw col-val (first opts)))

(comment      
  (insert-batch table key {(strify parent) (map (fn [k v]
                                                  {:name (strify k)
                                                   :value (->bytes v)
                                                   :timestamp (System/currentTimeMillis)})
                                                ks vs)}))

(defmulti get family-type-and-single-or-batch)
(defmethod get [::standard ::single]
  [table key family-kw col-kw & opts]
  (get-column table key family-kw col-kw (first opts)))
(defmethod get [::standard ::batch]
  [table key family-kw col-kws & opts]
  (get-slice-by-names table key family-kw col-kws (first opts)))

(defmulti remove (fn [x & r] (:type x)))
(defmethod remove ::family
  ([family key]
     1)
  ([family key col]
     2))
(defmethod remove :default
  ([client table key parent]
     (.remove client table key parent (System/currentTimeMillis) -1))
  ([client table key parent col]
     (.remove client table key (str parent ":" (strify col)) (System/currentTimeMillis) -1)))
