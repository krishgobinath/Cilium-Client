package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ciliumk8s "github.com/cilium/cilium/pkg/k8s"
	ciliumcs "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
)

func main () {

	ciliumk8s.Configure("", "/Users/gk/.kube/config", 0, 0)
	ciliumConfig, ok := ciliumk8s.CreateConfig()
	if ok != nil {
		fmt.Println("Failed to get Cilium Config.")
		return
	}
	ciliumclientSet := ciliumcs.NewForConfigOrDie(ciliumConfig)
	ceps, ok := ciliumclientSet.CiliumV2().CiliumEndpoints("").List(context.Background(), metav1.ListOptions{})
	for i, cep := range ceps.Items {
		fmt.Printf("[%d] %s\n", i, cep.GetName())
	}
}