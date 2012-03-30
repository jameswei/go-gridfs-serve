package main

import (
  "flag"
  "fmt"
  "io"
  "net/http"
  "net/http/pprof"
  "os"
  "runtime"
  "strconv"
  "strings"
  "time"
  "launchpad.net/mgo"
  "launchpad.net/mgo/bson"
)

var cpucores = flag.Int("cpucores", 1, "specify how many cpu cores to use")
var collection = flag.String("collection", "fs", "name of the GridFS collection")
var database = flag.String("database", "", "name of the GridFS database")
var host = flag.String("host", "127.0.0.1", "host of the GridFS database")
var port = flag.String("port", "27017", "port of the GridFS database")
var listen = flag.String("listen", ":8080", "adress:port to listen to")
var debug = flag.Bool("debug", false, "Turn on profiling via http")
var ensureIndex = flag.Bool("index", false, "Turns on index creation at start")

type Config struct {
  Collection string
  Database string
}

const BUFF_SIZE int = 1024 * 64 // 64Kbyte
var CPUCORES = 2
var CONFIG *Config
var SESSION *mgo.Session

func handler(w http.ResponseWriter, r *http.Request) {
  if r.Method != "GET" {
    w.WriteHeader(http.StatusMethodNotAllowed)
    return
  }

  s := SESSION.Copy()
  etag := r.Header.Get("If-None-Match")
  if etag != "" {
    tag := strings.Split(etag, "_")
    if len(tag) == 2 {
      etag_id, etag_md5 := tag[0], tag[1]
      count, err := s.DB(CONFIG.Database).C(CONFIG.Collection+".files").Find(
                bson.M{"_id": bson.ObjectIdHex(etag_id), "md5": etag_md5}).Count()
      if count > 0 && err == nil {
        w.Header().Set("Cache-Control", "max-age=2629000: public") // 1 Month
        w.Header().Set("ETag", etag)
        w.WriteHeader(http.StatusNotModified)
        return
      }
    }
  }

  split := strings.Split(r.URL.Path, "/")
  file_request := split[len(split)-1:][0]

  if file_request == "favicon.ico" {
    w.WriteHeader(http.StatusNotFound)
    return
  }

  gridfs := s.DB(CONFIG.Database).GridFS(CONFIG.Collection)
  file, err := gridfs.Open(file_request)

  if err != nil || file == nil {
    w.WriteHeader(http.StatusNotFound)
    return
  }

  etag = file.Id().(bson.ObjectId).Hex() + "_" + file.MD5()
  w.Header().Set("ETag", etag)
  w.Header().Set("Cache-Control", "max-age=2629000: public") // 1 Month
  w.Header().Set("Content-MD5", file.MD5())

  contentType := file.ContentType()
  if contentType != "" {
    w.Header().Set("Content-Type", contentType)
  } else {
    w.Header().Set("Content-Type","application/octet-stream")
  }

  w.Header().Set("Content-Length", strconv.FormatInt(file.Size(), 10))

  var n int
  var buf = make([]byte, BUFF_SIZE)
  for {
    n, err = file.Read(buf)
    if n == 0 && err != nil {
      break
    } else {
      w.Write(buf[:n])
    }
   }
   if err != io.EOF {
     panic(err)
   }

  defer file.Close()
  defer s.Close()
}

func main() {
  CONFIG = new (Config)
  flag.Parse()
  if *collection != "" {
    CONFIG.Collection = *collection
  }
  if *database != "" {
    CONFIG.Database = *database
  } else {
    fmt.Println("Error: You need to specify a database name!")
    os.Exit(-1)
  }

  fmt.Println("go-grid-serve version:", "v0.1.0")
  fmt.Println("go version:", runtime.Version())
  fmt.Printf("using %d CPU cores\n", *cpucores)
  runtime.GOMAXPROCS(*cpucores )

  SESSION, _ = mgo.Dial(fmt.Sprintf("mongodb://%s:%s?connect=direct", *host, *port))
  SESSION.SetMode(mgo.Monotonic, true)
  collection := SESSION.DB(CONFIG.Database).C(CONFIG.Collection+".files")


  if *ensureIndex {
    filenameIndex := mgo.Index{Key: []string{"filename", "-created"}, Unique: false, DropDups: false, Background: true, Sparse: false }
    md5Index := mgo.Index{Key: []string{"_id", "md5"}, Unique: false, DropDups: false, Background: true, Sparse: false }
    err := collection.EnsureIndex(filenameIndex)
    if err != nil {
      panic(err)
      os.Exit(-1)
    }
    err = collection.EnsureIndex(md5Index)
    if err != nil {
      panic(err)
      os.Exit(-1)
    }
  }

  mongoHandler := http.NewServeMux()
  mongoHandler.HandleFunc("/", handler)

  if *debug {
    mongoHandler.HandleFunc("/debug/pprof/", pprof.Index)
    mongoHandler.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    mongoHandler.HandleFunc("/debug/pprof/profile", pprof.Profile)
    mongoHandler.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
  }

  server := &http.Server{
    Addr:           *listen,
    Handler:        mongoHandler,
    ReadTimeout:    60 * time.Second,
    WriteTimeout:   10 * time.Second,
    MaxHeaderBytes: 32 * 1024,
  }
  fmt.Println(server.ListenAndServe())

  defer SESSION.Close()
}
