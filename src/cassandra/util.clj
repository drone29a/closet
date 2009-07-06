(ns cassandra.util)

(defn strify
  [keyword-or-other]
  (if (keyword? keyword-or-other)
    (name keyword-or-other)
    (str keyword-or-other)))

(defn canonicalize
  "Take a string keyword, make lowercase, then return it as a keyword."
  [s]
  (keyword "cassandra" (.toLowerCase s)))