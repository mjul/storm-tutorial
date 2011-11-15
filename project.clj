(defproject storm-tutorial "1.0.0"
  :description "Tutorial for the Storm stream processing library."
  :aot :all
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"] ;; for Leiningen, also defined for cake in .cake/config
  :dependencies [[clojure "1.2.0"]
                 [clj-time "0.3.0"]]
  :dev-dependencies [[storm "0.5.4"]])
