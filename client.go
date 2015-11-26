package main

import (
    "fmt"
    "hash/crc32"
    "math"
    "sort"
    "net/http"
    "errors"
    "io/ioutil"
    "encoding/json"
)
var(
    servers = []string{"http://localhost:3001","http://localhost:3000","http://localhost:3002"}
)
type keyval struct{
    key string
    value string
}

type Hashsort []uint32
func (h Hashsort) Len() int           { return len(h) }
func (h Hashsort) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h Hashsort) Less(i, j int) bool { return h[i] < h[j] }

type Chash struct{
    circle map[uint32]string
    sortedKeys []uint32
    nodes []string
    weights map[string]int
}

type Response struct{
    Key string `json:"key"`
    Value string `json:"value"`
}

func New(nodes []string) *Chash {
    hashcircle := &Chash{
        circle:       make(map[uint32]string),
        sortedKeys: make([]uint32, 0),
        nodes:      nodes,
        weights:    make(map[string]int),
    }
    hashcircle.generateCircle()
    return hashcircle
}

//consisting hashing circle generator
func (h *Chash) generateCircle() {
    totalWeight := 0
    for _, node := range h.nodes {
        if weight, ok := h.weights[node]; ok {
            totalWeight += weight
        } else {
            totalWeight += 1
        }
    }

    for _, node := range h.nodes {
        weight := 1

        if _, ok := h.weights[node]; ok {
            weight = h.weights[node]
        }

        factor := math.Floor(float64(40*len(h.nodes)*weight) / float64(totalWeight))

        for j := 0; j < int(factor); j++ {
            nodeKey := fmt.Sprintf("%s-%d", node, j)
            bKey := hashDigest(nodeKey)

            for i := 0; i < 3; i++ {
                h.circle[bKey] = node
                h.sortedKeys = append(h.sortedKeys, bKey)
            }
        }
    }

    sort.Sort(Hashsort(h.sortedKeys))
}

//crc32 hash generator
func hashDigest(key string) uint32 {
    if len(key) < 64 {
        var scratch [64]byte
        copy(scratch[:], key)
        return crc32.ChecksumIEEE(scratch[:len(key)])
    }
    return crc32.ChecksumIEEE([]byte(key))
}

func (h *Chash) GetNode(stringKey string) (node string, ok bool) {
    pos, ok := h.GetNodePos(stringKey)
    if !ok {
        return "", false
    }
    return h.circle[h.sortedKeys[pos]], true
}

func (h *Chash) GetNodePos(stringKey string) (pos int, ok bool) {
    if len(h.circle) == 0 {
        return 0, false
    }

    key := h.GenKey(stringKey)

    nodes := h.sortedKeys
    pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > key })

    if pos == len(nodes) {
        // Wrap the search, should return first node
        return 1, true
    } else {
        return pos, true
    }
}

func (h *Chash) GenKey(key string) uint32 {
    bKey := hashDigest(key)
    return bKey
}

func putkey(x keyval)error{
    var url string
    ring := New(servers)
    server1,z := ring.GetNode(x.key)
    if(z==true){
        url =server1+"/"+"keys"+"/"+x.key+"/"+x.value }
    fmt.Println(url)
    req, err := http.NewRequest("PUT", url, nil)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    resp.Body.Close()

    // any status code 200..299 is "success", so fail on anything else
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return errors.New(http.StatusText(resp.StatusCode))
    }

    return nil
}

func getkey(x string)(Response, bool, error){
    var url string
    var u Response
    ring := New(servers)
    server1,z := ring.GetNode("x.key")
    if(z==true){
        url =server1+"/"+"keys"+"/"+x
    }
    fmt.Println(url)
    resp, err := http.Get(url)
    if err != nil || resp.StatusCode >= 400 {
        return Response{}, false, err
    }

    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return Response{}, false, err
    }
    err = json.Unmarshal(body, &u)
    if err != nil {
        return Response{}, false, err
    }
    return u, true, nil
}

func main(){
    var k []Response
    key := []keyval{{key:"1",value:"a"},
        {key:"2",value:"b"},
        {key:"3",value:"c"},
        {key:"4",value:"d"},
        {key:"5",value:"e"},
        {key:"6",value:"f"},
        {key:"7",value:"g"},
        {key:"8",value:"h"},
        {key:"9",value:"i"},
        {key:"10",value:"j"}}
    putkey(key[0])
    putkey(key[1])
    putkey(key[2])
    putkey(key[3])
    putkey(key[4])
    putkey(key[5])
    putkey(key[6])
    putkey(key[7])
    putkey(key[8])
    putkey(key[9])
    for i,_:=range key{
        x,y,z:=getkey(key[i].key)
        if(z==nil){
            if(y==true){
                k = append(k,x)
            }
        }
    }
    
    for i := range k {
        fmt.Println(k[i].Key + "=>" + k[i].Value)
    }
}