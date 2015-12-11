package main

import (
	"flag"
	"log"
	"net/http"
	"html/template"
	"sync"
	"path/filepath"
	"github.com/gorilla/websocket"
	diff "github.com/falconandy/sqlserver-index-diff"
//	"encoding/json"
)

type templateHandler struct {
	once     sync.Once
	filename string
	templ    *template.Template
}

func (t *templateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.once.Do(func() {
		t.templ = template.Must(template.ParseFiles(filepath.Join("templates", t.filename)))
	})
	t.templ.Execute(w, r)
}

func main() {
	var addr = flag.String("addr", ":8080", "The addr of the application.")
	flag.Parse()

	http.Handle("/", &templateHandler{filename: "main.html"})
	http.HandleFunc("/diff", serveDiff)

	log.Println("Starting web server on", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

const (
	socketBufferSize  = 1024
	messageBufferSize = 256
)

var upgrader = &websocket.Upgrader{ReadBufferSize: socketBufferSize, WriteBufferSize: socketBufferSize}

func serveDiff(w http.ResponseWriter, req *http.Request) {
	socket, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Fatal("ServeHTTP:", err)
		return
	}

	for {
		var msg *message
		if err := socket.ReadJSON(&msg); err == nil {
			cfg1 := &diff.Config{SqlServer:msg.LeftServer, Database:msg.LeftDatabase}
			// cfg2 := &diff.Config{SqlServer:msg.RightServer, Database:msg.RightServer}
			indexes1 := diff.GetIndexes(cfg1.GetConnectionString())
			// indexes2 := diff.GetIndexes(cfg2.GetConnectionString())
			sortedIndexes1 := diff.GetSortedIndexes(indexes1)
			//sortedIndexes2 := diff.GetSortedIndexes(indexes2)
			//res, _ := json.Marshal(sortedIndexes1)
			log.Println(cfg1.SqlServer + "   " + cfg1.Database)
			log.Println(len(sortedIndexes1))
			socket.WriteJSON(sortedIndexes1)
		} else {
			break
		}
	}
	socket.Close()
}
