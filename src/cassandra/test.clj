(ns cassandra.test
  (:require [cassandra :as cdb])
  (:use clojure.test))

(def *db* nil)
(def *table* nil)

(def test-data
     {:standard {:dog {:sounds {:growl "angry"
                                :bark "attention"
                                :laugh "happy"
                                :pant "hot"}}
                 :cat {:sounds {:growl "angry"
                                :hiss "really angry"
                                :meow "feed me, slave"}}}
      :super {:dog {:breeds {:boxer {:color ["fawn" "brindle"]
                                     :weight {:male [65 75]
                                              :female [50 60]}}
                             :norwegian-elkhound {:color ["gray"]
                                                  :weight {:male [50 60]
                                                            :female [40 50]}}}}}})

(defn init-db
  [f]
  (binding [*db* (cdb/make-client "192.168.50.128" 9160)
            *table* (-> *db* :tables :animals)]
    (doseq [[k v] (:standard test-data)]
      (doseq [[f cs] v]
        (println f cs)
        (cdb/put *table* k f cs)))
    (f)
    (.close (:thrift-client *db*))))

(use-fixtures :once init-db)

(deftest get-standard-single
  (is (= "angry" 
         (cdb/get *table* "dog" :sounds [:growl]))))

(deftest get-standard-slice
  (is (= {:growl "angry" :laugh "happy"} 
         (cdb/get *table* "dog" :sounds [:growl :laugh])))
  (is (= {:growl "angry" :laugh "happy"}
         (cdb/get *table* "dog" :sounds {:start :growl :finish :pant :count 2})))
  (is (= {:bark "attention" :growl "angry" :laugh "happy" :pant "hot"}
         (cdb/get *table* "dog" :sounds)))
  (is (= {:bark "attention" :growl "angry" :laugh "happy"}
         (cdb/get *table* "dog" :sounds {:start nil :finish nil :count 3}))))

(deftest get-super-single
  (is (= {:boxer {:color ["fawn" "brindle"]
                  :weight {:male [65 75]
                           :female [50 60]}}}
         (cdb/get *table* "dog" :breeds [:boxer]))))
(deftest get-super-slice
  (is (= {:boxer {:color ["fawn" "brindle"]
                  :weight {:male [65 75]
                           :female [50 60]}}
          :norwegian-elkhound {:color ["gray"]
                               :weight {:male [50 60]
                                        :female [40 50]}}}
         (cdb/get *table* "dog" :breeds)))

  (is (= {:boxer {:color ["fawn" "brindle"]
                  :weight {:male [65 75]
                           :female [50 60]}}
          :norwegian-elkhound {:color ["gray"]
                               :weight {:male [50 60]
                                        :female [40 50]}}}
         (cdb/get *table* "dog" :breeds [:boxer :norwegian-elkhound])))
  ;; need to add timeline order test
  )
(deftest put-standard-single
  (cdb/put *table* "dog" :sounds {:sigh "fatigue"})
  (is (= {:sigh "fatigue"} 
         (cdb/get *table* "dog" :sounds [:sigh])))
  
  (cdb/put *table* "dog" :sounds {:bark "warning"})
  (is (= {:bark "warning"}
         (cdb/get *table* "dog" :sounds [:bark]))))

(deftest put-standard-batch
  (cdb/put *table* "dog" :sounds {:woof "questioning"
                                  :boof "confirmation"})
  (is (= {:woof "questioning"
          :boof "confirmation"}
         (cdb/get *table* "dog" :sounds [:woof :boof])))

  (cdb/put *table* "dog" :sounds {:woof "concern" 
                                  :yip "ouch"})
  (is (= {:woof "concern"
          :yip "ouch"}
         (cdb/get *table* "dog" :sounds [:woof :yip]))))

(deftest put-super-single
  (cdb/put *table* "dog" :breeds {:boxer {:color ["fawn" "brindle" "white"]
                                          :weight {:male [65 75]
                                                   :female [50 60]}}})
  (is (= {:boxer {:color ["fawn" "brindle" "white"]
                  :weight {:male [65 75]
                           :female [50 60]}}}
         (cdb/get *table* "dog" :breeds [:boxer]))))

(deftest put-super-batch
 (cdb/put *table* "dog" :breeds {:boxer {:color ["fawn" "brindle"]
                                         :weight {:male [65 75]
                                                  :female [50 60]}}
                                 :norwegian-elkhound {:color ["gray"]
                                                      :weight {:male [50 60]
                                                               :female [40 50]}}})
 (is (= {:boxer {:color ["fawn" "brindle"]
                 :weight {:male [65 75]
                          :female [50 60]}}
         :norwegian-elkhound {:color ["gray"]
                              :weight {:male [50 60]
                                       :female [40 50]}}}
        (cdb/get *table* "dog" :breeds [:boxer :norwegian-elkhound]))))

(deftest get-tests
  (get-standard-single)
  (get-standard-slice)
  (get-super-single)
  (get-super-slice))

(deftest put-tests
  (put-standard-single)
  (put-standard-batch)
  (put-super-single)
  (put-super-batch))

(defn test-ns-hook
  []
  (get-tests)
  (put-tests))