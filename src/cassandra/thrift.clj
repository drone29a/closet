(ns 
    #^{:author "Matt Revelle"
       :doc "Calls to Cassandra's Thrift client"} 
  cassandra.thrift
  (:use [clojure.contrib.str-utils :only [re-split]]
        cassandra.util)
  (:import (org.apache.cassandra.service Cassandra$Client column_t batch_mutation_t)
           (org.apache.thrift.transport TSocket)
           (org.apache.thrift.protocol TBinaryProtocol)))

(declare col-path
         map->column_t
         column_t->map
         decode-column)

(defn insert-single
  [table key family-kw col-val & opts]
  (let [encoder (or (:encoder opts)
                    (-> table :families family-kw :encoder)
                    (:encoder table))
        timestamp (or (:timestamp opts)
                      (System/currentTimeMillis))
        block-for (or (:block-for opts)
                      -1)]
    (.insert (:client table) 
             (name (:name table)) 
             key 
             (col-path (name family-kw) (name (first (keys col-val))))
             (encoder (first (vals col-val))) 
             timestamp 
             block-for)))

(defn insert-batch
  ([client table key cfmap]
     (insert-batch client table key cfmap -1))
  ([client table key cfmap block-for]
     (let [cfm (into {} (map (fn [[k v]] 
                               [k (map map->column_t v)])
                             cfmap))]
       (.batch_insert client (batch_mutation_t. table key cfm) block-for))))

(defn get-column
  ([table key family-kw col-kw & opts]
     (let [client (:client table)
           decoder (or (:decoder opts)
                       (-> table :families family-kw :decoder)
                       (:decoder table))]
       (decode-column decoder 
                      (column_t->map (.get_column client 
                                                  (name (:name table)) 
                                                  key 
                                                  (col-path (name family-kw) (name col-kw))))))))

(defn get-slice
  [table key family-kw & opts]
  (let [opts (first opts)
        start (get opts :start -1)
        count (get opts :count -1)
        decoder (or (:decoder opts)
                    (-> table :families family-kw :decoder)
                    (:decoder table))]
    (map (comp #(decode-column decoder %) column_t->map) 
         (.get-slice (:client table) 
                     (name (:name table)) 
                     key 
                     (name family-kw) 
                     start 
                     count))))

(defn get-slice-by-names
  [table key family-kw col-kws & opts]
  (let [decoder (or (:decoder opts)
                    (-> table :families family-kw :decoder)
                    (:decoder table))]
    (map (comp #(decode-column decoder %) column_t->map) 
         (.get_slice_by_names (:client table) 
                              (name (:name table)) 
                              key 
                              (name family-kw)
                              (map name col-kws)))))

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