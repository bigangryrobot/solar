package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type dataRecord struct {
	data       []byte
	generation int
	dataMutex  sync.RWMutex
}

func (n *dataRecord) setData(data []byte) {
	n.dataMutex.Lock()
	defer n.dataMutex.Unlock()
	n.data = data
	n.generation = n.generation + 1
}

func (n *dataRecord) getData() ([]byte, int) {
	n.dataMutex.RLock()
	defer n.dataMutex.RUnlock()
	return n.data, n.generation
}

func (n *dataRecord) notifyData(curData []byte, curGeneration int) bool {
	if curGeneration > n.generation {
		n.dataMutex.Lock()
		defer n.dataMutex.Unlock()
		n.generation = curGeneration
		n.data = curData
		return true
	}
	return false
}

const ReplicationFactor = 2

func main() {
	cluster, err := setupCluster("127.0.0.1", "")
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Leave()

	dr := &dataRecord{}
	launchHTTPAPI(dr)

	ctx := context.Background()
	if name, err := os.Hostname(); err == nil {
		ctx = context.WithValue(ctx, "name", name)
	}

	debugDataPrinterTicker := time.Tick(time.Second * 5)
	dataBroadcastTicker := time.Tick(time.Second * 2)
	for {
		select {
		case <-dataBroadcastTicker:
			members := getOtherMembers(cluster)

			ctx, _ := context.WithTimeout(ctx, time.Second*2)
			go notifyOthers(ctx, members, dr)

		case <-debugDataPrinterTicker:
			log.Printf("Members: %v\n", cluster.Members())

			curData, curGen := dr.getData()
			log.Printf("State: Record: %v Gen: %v\n", curData, curGen)
		}
	}
}

func setupCluster(advertiseAddr string, clusterAddr string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	_, err = cluster.Join([]string{clusterAddr}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}

func launchHTTPAPI(dr *dataRecord) {
	go func() {
		m := mux.NewRouter()
		m.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
			val, _ := dr.getData()
			fmt.Fprintf(w, "%v", string(val))
		})
		m.HandleFunc("/set/{data}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			data, err := base64.URLEncoding.DecodeString(vars["data"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}

			dr.setData(data)

			decodedval := []byte{}
			base64.URLEncoding.Decode(decodedval, data)

			fmt.Fprintf(w, "%v", decodedval)
		})
		m.HandleFunc("/notify/{curData}/{curGeneration}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			curData := []byte(vars["curData"])
			curGeneration, err := strconv.Atoi(vars["curGeneration"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}

			if changed := dr.notifyData(curData, curGeneration); changed {
				log.Printf(
					"NewRecord: %v Gen: %v Notifier: %v",
					curData,
					curGeneration,
					r.URL.Query().Get("notifier"))
			}
			w.WriteHeader(http.StatusOK)
		})
		log.Fatal(http.ListenAndServe(":8080", m))
	}()
}

func getOtherMembers(cluster *serf.Serf) []serf.Member {
	members := cluster.Members()
	for i := 0; i < len(members); {
		if members[i].Name == cluster.LocalMember().Name || members[i].Status != serf.StatusAlive {
			if i < len(members)-1 {
				members = append(members[:i], members[i+1:]...)
			} else {
				members = members[:i]
			}
		} else {
			i++
		}
	}
	return members
}

func notifyOthers(ctx context.Context, otherMembers []serf.Member, dr *dataRecord) {
	g, ctx := errgroup.WithContext(ctx)

	if len(otherMembers) <= 2 {
		for _, member := range otherMembers {
			curMember := member
			g.Go(func() error {
				return notifyMember(ctx, curMember.Addr.String(), dr)
			})
		}
	} else {
		randIndex := rand.Int() % len(otherMembers)
		for i := 0; i < ReplicationFactor; i++ {
			curIndex := i
			g.Go(func() error {
				return notifyMember(
					ctx,
					otherMembers[(randIndex+curIndex)%len(otherMembers)].Addr.String(),
					dr)
			})
		}
	}

	err := g.Wait()
	if err != nil {
		log.Printf("Error when notifying other members: %v", err)
	}
}

func notifyMember(ctx context.Context, addr string, dr *dataRecord) error {
	val, gen := dr.getData()
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%v:8080/notify/%v/%v?notifier=%v", addr, val, gen, ctx.Value("name")), nil)
	if err != nil {
		return errors.Wrap(err, "Couldn't create request")
	}
	req = req.WithContext(ctx)

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "Couldn't make request")
	}
	return nil
}
