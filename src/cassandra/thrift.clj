(ns 
    #^{:author "Matt Revelle"
       :doc "Calls to Cassandra's Thrift client"} 
  cassandra.thrift
  (:refer-clojure :exclude [remove])
  (:use [clojure.contrib.str-utils :only [re-split]]
        cassandra.util)
  (:import (org.apache.cassandra.service Cassandra$Client column_t batch_mutation_t)
           (org.apache.thrift.transport TSocket)
           (org.apache.thrift.protocol TBinaryProtocol)))

(declare col-path
         map->column_t
         column_t->map
         decode-column
         parse-table-schema)

(defn insert-single
  [client table key family col-val encoder & opts]
  (let [timestamp (get opts :timestamp (System/currentTimeMillis))
        block-for (get opts :block-for 0)]
    (.insert client 
             table 
             key 
             (col-path family (first (keys col-val)))
             (encoder (first (vals col-val))) 
             timestamp 
             block-for)))

(defn insert-batch
  [client table key fam-col-val encoder & opts]
  (let [opts (first opts)
        block-for (get opts :block-for 0)
        fam-col-val (into {} (map (fn [[k v]] 
                                    [k (map (fn [m] 
                                              (map->column_t (assoc m :value 
                                                                    (encoder (:value m))))) 
                                            v)])
                          fam-col-val))]
    (.batch_insert client (batch_mutation_t. table key fam-col-val) block-for)))

(defn get-column
  [client table key family col decoder]
  (decode-column decoder 
                 (column_t->map (.get_column client 
                                             table 
                                             key 
                                             (col-path family col)))))

(defn get-slice
  [client table key family decoder & opts]
  (let [opts (first opts)
        ascending? (get opts :start true)
        count (get opts :count -1)]
    (map (comp #(decode-column decoder %) column_t->map) 
         (.get-slice (:client table) 
                     table 
                     key 
                     family 
                     ascending?
                     count))))

(defn get-slice-by-names
  [client table key family cols decoder & opts]
  (map (comp #(decode-column decoder %) column_t->map) 
       (.get_slice_by_names client
                            table 
                            key 
                            family
                            cols)))

(defn remove
  [client table key col-path-or-parent & opts]
  (let [opts (first opts)
        timestamp (get opts :timestamp (System/currentTimeMillis))
        block-for (get opts :block-for 0)]
    (.remove client
             table
             key
             col-path-or-parent
             timestamp
             block-for)))

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

(defn parse-table-schema
  "Parse the schema for a table description."
  [desc]
  (into {} (map (fn [family-desc]
                  (let [lines (re-split #"\n" family-desc)
                        name (second (re-find #"\.([^\(]+)\(" (nth lines 0)))
                        type (second (re-find #":\s(.*)$" (nth lines 1)))
                        sorted-by (second (re-find #":\s(.*)$" (nth lines 2)))]
                    [(keyword name) {:type (canonicalize type)
                                     :sorted-by (canonicalize sorted-by)}]))
                (re-split #"-----\n" desc))))

(defn decode-column
  [decoder col]
  (assoc col :value (decoder (:value col))))