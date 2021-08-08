package main

import (
	"albertlockett.ca/cloud-native-go/kv"
	"errors"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
)

func kvGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := kv.Get(key)
	if errors.Is(err, kv.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value))
}

func kvPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = kv.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	txlogger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
}

func kvDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if err := kv.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	txlogger.WriteDelete(key)
	w.WriteHeader(http.StatusOK)
}

func main() {
	initializeTransactionLog()
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", kvGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", kvPutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", kvDeleteHandler).Methods("DELETE")
	log.Fatal(http.ListenAndServe(":8080", r))
}