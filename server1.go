package main

import (
    "fmt"
    "net/http"
    "encoding/json"
    "github.com/julienschmidt/httprouter"
)

var keyValPair map[string]string

type error struct {
    Error_message string `json:"error_message"`
}

type Response struct {
    Key string `json:"key"`
    Value string `json:"value"`
}

func addValue(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	key_id :=  p.ByName("key_id")
    value :=  p.ByName("value")

    keyValPair[key_id] = value

    rw.WriteHeader(200)
}

func getKeyValue(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	var response []byte
    key_id :=  p.ByName("key_id")
    if value,found := keyValPair[key_id]; found {
        jsonResponse := &Response{
            key_id,
            value,
        }
        resp,_ := json.Marshal(jsonResponse)
        response = resp
    }else {
        jsonResponse := &error{
            "Key does not found in the cache",
        }
        resp,_ := json.Marshal(jsonResponse)
        response = resp
    }

    // Write content-type, statuscode, payload
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", response)
}

func getAllKeyValues(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {

	var output[] Response
    for key,val := range keyValPair{
        jsonResponse := Response{
            Key :key,
            Value:val,
        }
        output = append(output, jsonResponse)
    }

    resp,_ := json.Marshal(output)

    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resp)
}

func main() {
	fmt.Println("Server is running on 3001!")

	keyValPair = make(map[string]string)

    mux := httprouter.New()

    mux.PUT("/keys/:key_id/:value", addValue)

    mux.GET("/keys/:key_id", getKeyValue)

    mux.GET("/keys", getAllKeyValues)

    server := http.Server{
            Addr:        "0.0.0.0:3001",
            Handler: mux,
    }
    server.ListenAndServe()
}