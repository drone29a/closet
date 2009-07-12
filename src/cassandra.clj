(ns cassandra
  (:refer-clojure :exclude [get remove])
  (:require [cassandra.thrift :as thrift])
  (:use [clojure.contrib.json.read :only [read-json-string]] 
        [clojure.contrib.json.write :only [json-str]] 
        cassandra.util)
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
  (let [schema (thrift/parse-table-schema (.describeTable client table-name))]
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
  (let [opts (first opts)
        encoder (or (:encoder opts)
                    (-> table :families family-kw :encoder)
                    (-> table :encoder))]
    (thrift/insert-single (-> table :client) 
                          (name (:name table)) 
                          (strify key) 
                          (name family-kw) 
                          (zipmap (map name (keys col-val)) (vals col-val)) 
                          encoder
                          opts)))

(defmethod put [::standard ::batch]
  [table key family-kw cols-vals & opts]
  (let [opts (first opts)
        encoder (or (:encoder opts)
                    (-> table :families family-kw :encoder)
                    (-> table :encoder))]
    (thrift/insert-batch (-> table :client)
                         (name (:name table)) 
                         (strify key) 
                         {(name family-kw) (map (fn [[k v]]
                                                  {:name (strify k)
                                                   :value v
                                                   :timestamp (System/currentTimeMillis)})
                                                cols-vals)}
                         encoder
                         opts)))

(defmulti get family-type-and-single-or-batch)

(defmethod get [::standard ::single]
  [table key family-kw col-kw & opts]
  (let [opts (first opts)
        decoder (or (:decoder opts)
                    (-> table :families family-kw :decoder)
                    (-> table :decoder))
        as-column? (:as-column? opts)
        col (thrift/get-column (-> table :client)
                               (name (:name table)) 
                               (strify key) 
                               (name family-kw) 
                               (name col-kw)
                               decoder)]
    (if as-column?
      col
      (hash-map (keyword (:name col)) (:value col)))))

(defmethod get [::standard ::batch]
  [table key family-kw col-kws & opts]
  (let [opts (first opts)
        decoder (or (:decoder opts)
                    (-> table :families family-kw :decoder)
                    (-> table :decoder))
        as-columns? (:as-columns? opts)
        cols (thrift/get-slice-by-names (-> table :client)
                                        (name (:name table)) 
                                        (strify key) 
                                        (name family-kw) 
                                        (map name col-kws) 
                                        decoder)]
    (if as-columns?
      cols
      (into {} (map (fn [c]
                      [(keyword (:name c)) (:value c)])
                    cols)))))

(defn remove
  ([table key family-kw]
     (thrift/remove (:client table) 
                    (name (:name table)) 
                    (strify key) 
                    (name family-kw)))
  ([table key family-kw col-kw]
     (thrift/remove (:client table) 
                    (name (:name table)) 
                    (strify key) 
                    (thrift/col-path (name family-kw) (strify col-kw)))))
