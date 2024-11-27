package httpd


import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

//define the interface to interact with key-value database
type Database interface {
	Get(key string) (string, error) //returns string val given a key

	Set(key, value string) error //sets the value for a key

	Delete(key, string) error //deletes key

	Join(nodeID string, addr string) error //joins nodeID to the cluster

}

//Wrapper around Store interface
type HTTPService struct {
	addr string
	ln   net.Listener

	database Database
}

//returns a pointer to a newly created servicer
func createService(addr string, database Database) *HTTPService {
	return &HTTPService{
		addr:  addr,
		database: database,
	}
}

//start a servicer
func (s *HTTPService) Start() error {
	server := http.Server{ //create server
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr) //listen for someone that tries to invoke service
	if err != nil {
		return err
	}
	s.ln = ln
	http.Handle("/", s) //handle

	//start a thread to handle request
	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil

//close the service
func (s *HTTPService) Close() {
	s.ln.Close()
	return
}

//getter for the address of someone interacting with the service
func (s *HTTPService) Addr() net.Addr {
	return s.ln.Addr()
}


func (s *HTTPService) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	//grab the key from request
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	//handle get and post request (get key and set value, respectively)
	switch r.Method {
	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		v, err := s.database.Get(k) //grab the value
		if err != nil { //if erred, print the error and return
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		//otherwise, write value as response
		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, string(b))

	case "POST":
		//Grab the value from the post body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.database.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.store.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}




