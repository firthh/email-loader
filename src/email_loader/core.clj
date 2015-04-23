(ns email-loader.core
  (:use [clojure.pprint])
  (:require [simple-queue.core :as q]
            [clojure.data.json :as json]
            [clj-http.client :as client]
            [clojure.string :as str]
            [monger.core :as mg]
            [monger.collection :as mc]))

(def connection (mg/connect))

(def db (mg/get-db connection "jive-emails"))

(defn insert-emails [emails]
  (if (> (count emails) 0)
    (mc/insert-batch db "emails" emails)))

(defn get-data [url]
  (-> (client/get url {:throw-exceptions false})
      :body))

(defn remove-error-statement [string]
  (str/replace-first string #"throw 'allowIllegalResourceCall is false.';" ""))

(defn get-next [response at]
  (reset! at (:next (:links response)))
  response)

(defn load-data [download-url]
  (mc/drop db "emails")
  (loop [url download-url]
    (let [next (atom nil)]
      (println "getting data from url " url)
      (-> (get-data url)
          remove-error-statement
          (json/read-str :key-fn keyword)
          (get-next next)
          :list
          insert-emails)
      (if (not (nil? @next))
        (recur @next)))))

(q/defhandler message-handler
  (println "-----------")
  (let [message (json/read-str data :key-fn keyword)]
    (pprint message)
    (if (= "download-new-messages" (:event message))
      (do (load-data (:available-at message))
          (q/publish component (json/write-str {:event "emails-downloaded"})))
      )))
;;"https://community.jivesoftware.com/api/core/v3/places/152179/contents?count=10&startIndex=0"
(defn -main [& args]
  (let [component (q/create-queue "test" {:uri (first args)})]
    (println  " [*] Waiting for messages... To exit press CTRL+C")
    (q/subscribe component (message-handler component))

    (.stop component)))
