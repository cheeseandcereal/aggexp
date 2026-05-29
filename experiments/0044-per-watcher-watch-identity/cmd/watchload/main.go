// Command watchload is a scenario-2/3/4 load generator for experiment
// 0044. It opens N concurrent watches against aggexp.io/v1 widgets as
// an impersonated identity (or a set of identities) and holds them
// open for a duration, so the AA's backend-call counters can be read
// from its logs as a function of N and mode.
//
// Not part of the served apiserver; a throwaway lab tool.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		n         = flag.Int("n", 1, "number of concurrent watches")
		ns        = flag.String("namespace", "demo", "namespace")
		identity  = flag.String("as", "alice", "single impersonated user (used when -identities empty)")
		identCSV  = flag.String("identities", "", "comma list of users to round-robin across the N watches")
		hold      = flag.Duration("hold", 30*time.Second, "how long to hold the watches open")
		kubeconfig = flag.String("kubeconfig", os.Getenv("HOME")+"/.kube/config", "kubeconfig")
		label     = flag.String("selector", "", "label selector applied to every watch")
	)
	flag.Parse()

	idents := []string{*identity}
	if *identCSV != "" {
		idents = splitCSV(*identCSV)
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		os.Exit(1)
	}

	gvr := schema.GroupVersionResource{Group: "aggexp.io", Version: "v1", Resource: "widgets"}
	ctx, cancel := context.WithTimeout(context.Background(), *hold+30*time.Second)
	defer cancel()

	var events int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(*hold)
	for i := 0; i < *n; i++ {
		who := idents[i%len(idents)]
		ic := *cfg
		ic.Impersonate = rest.ImpersonationConfig{UserName: who}
		dyn, err := dynamic.NewForConfig(&ic)
		if err != nil {
			fmt.Fprintln(os.Stderr, "client:", err)
			os.Exit(1)
		}
		w, err := dyn.Resource(gvr).Namespace(*ns).Watch(ctx, metav1.ListOptions{LabelSelector: *label})
		if err != nil {
			fmt.Fprintf(os.Stderr, "watch %d (%s): %v\n", i, who, err)
			os.Exit(1)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-w.ResultChan():
					if !ok {
						return
					}
					atomic.AddInt64(&events, 1)
				case <-time.After(time.Until(deadline)):
					return
				}
			}
		}()
	}
	fmt.Printf("opened %d watches (identities=%v) holding %s\n", *n, idents, *hold)
	time.Sleep(time.Until(deadline))
	cancel()
	wg.Wait()
	fmt.Printf("closed %d watches; total events observed=%d\n", *n, atomic.LoadInt64(&events))
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
