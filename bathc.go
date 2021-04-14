package endpointbatch

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	cilium_api_v2 "github.com/cilium/cilium/pkg/k8s/apis/cilium.io/v2"
	cilium_v2 "github.com/cilium/cilium/pkg/k8s/apis/cilium.io/v2"
	clientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned/typed/cilium.io/v2"
)

var (
	apiServerBackoff = wait.Backoff{
		Steps:    4,
		Duration: 10 * time.Millisecond,
		Factor:   5.0,
		Jitter:   0.1,
	}
	cacheCep = make(map[string]string)
	cacheCeb = make(map[string]*CebBatch)
)

const (
	cebNamePrefix    = "ceb"
	syncStateNever   = 0
	syncStatePartial = 1
	syncStateDone    = 2
	maxCepsInCeb     = 100
	numRetries       = 5
)

type CebBatch struct {
	Ceb        cilium_api_v2.CiliumEndpointBatch
	client     clientset.CiliumV2Interface
	StopCh     chan struct{}
	syncState  int
	syncPeriod time.Duration
}

// Generate random string for given length of characters.
func randomName(n int) string {
	var letters = []rune("bcdfghjklmnpqrstvwxyz2456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// Get unique name for cebBatch
func uniqueCebBatchName(cnt int) string {
	cebName := fmt.Sprintf("%s-%s-%s", cebNamePrefix, randomName(cnt), randomName(4))
	for _, ok := cacheCeb[cebName]; ok; {
		cebName := fmt.Sprintf("%s-%s-%s", cebNamePrefix, randomName(cnt), randomName(4))
		_, ok = cacheCeb[cebName]
	}

	return cebName
}

// Create new CebBatch
func NewCebBatch(client clientset.CiliumV2Interface, name string) *CebBatch {
	var cebName string = name
	if name == "" {
		cebName = uniqueCebBatchName(10)
	}
	log.Infof("Generated cebName:%s", cebName)
	ceb := &CebBatch{
		Ceb: cilium_api_v2.CiliumEndpointBatch{
			ObjectMeta: metav1.ObjectMeta{
				Name: cebName,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v2",
						Kind:       "CiliumEndpointBatch",
						Name:       cebName,
					},
				},
			},
		},
		client:     client,
		StopCh:     make(chan struct{}),
		syncState:  syncStateNever,
		syncPeriod: 15 * time.Second,
	}

	return ceb
}

func (c *CebBatch) RunUntil() {
	// Retry updating upStream, until it sync timeout
	wait.Until(func() {

		// If local cached data is already synced to upstream
		// wait for new data
		if c.syncState == syncStateDone {
			return
		}
		// sync local cilium endpoint batch to upstream(APIServer)
		// If there is any error from ApiServer, keep trying.
		if err := c.updateCeb(); err != nil {
			log.WithError(err).Info("Failed to update CEB on first attempt:%s\n", c.Ceb.GetName())
			if ok := c.onError(); ok != nil {
				return
			}
		}
		c.syncState = syncStateDone
		return
	}, c.syncPeriod, c.StopCh)
}

func (c *CebBatch) onError() error {
	err := wait.ExponentialBackoff(apiServerBackoff, func() (bool, error) {
		log.Info("Retrying to update CEB:%s\n", c.Ceb.GetName())
		if ok := c.updateCeb(); ok != nil {
			log.WithError(ok).Info("Failed to update CEB:%s\n", c.Ceb.GetName())
			return false, nil
		}
		return true, nil
	})
	return err
}

func (c *CebBatch) updateCeb() error {
	// Update existing ceb, if already exists one.
	var err error
	if c.syncState == syncStateNever {
		log.Infof("Creating a new CEB :%s\n", c.Ceb.GetName())
		_, err = c.createCiliumEndpointBatch()
	} else {
		log.Infof("Updating the CEB :%s\n", c.Ceb.GetName())
		_, err = c.updateCiliumEndpointBatch()
	}

	return err
}

// Create a cilium Endpoint Object
func (c *CebBatch) createCiliumEndpointBatch() (*cilium_api_v2.CiliumEndpointBatch, error) {
	return c.client.CiliumEndpointBatches().Create(context.TODO(), &c.Ceb, metav1.CreateOptions{})
}

// Update the cilium Endpoint Object
func (c *CebBatch) updateCiliumEndpointBatch() (*cilium_api_v2.CiliumEndpointBatch, error) {
	return c.client.CiliumEndpointBatches().Update(context.TODO(), &c.Ceb, metav1.UpdateOptions{})
}

// Delete the cilium Endpoint Object
func (c *CebBatch) DeleteCiliumEndpointBatch() error {
	return c.client.CiliumEndpointBatches().Delete(context.TODO(), c.Ceb.GetName(), metav1.DeleteOptions{})
}

func BatchCepIntoCeb(client clientset.CiliumV2Interface, cep *cilium_v2.CiliumEndpoint) error {

	fmt.Println("Entering BatchCepIntoCeb ", cep.GetName())
	// Check in local cache, if a given cep is already processed by one of the ceb.
	// and if exists, update a ceb with the new cep object in it.
	if cebName, ok := cacheCep[cep.GetName()]; ok {
		log.Infof("Inserting CEP :%s in CEB: %s\n", cep.GetName(), cebName)
		queueCepInCeb(cep, cacheCeb[cebName])
		return nil
	}

	// find the matching ceb for the cep
	ceb, ok := getCeb(client, cep)
	if ok != nil {
		log.WithError(ok).Errorf("Failed to get ceb for the cep: %s", cep.GetName())
	}

	fmt.Println("BatchCepIntoCeb cebNamr", ceb.Ceb.GetName())
	// Update ceb in local cache and batch into the ceb
	cacheCep[cep.GetName()] = ceb.Ceb.GetName()
	queueCepInCeb(cep, ceb)
	return nil
}

func getCeb(client clientset.CiliumV2Interface, cep *cilium_v2.CiliumEndpoint) (*CebBatch, error) {

	// Get the first available CEB
	for _, cebBatch := range cacheCeb {
		if len(cebBatch.Ceb.Endpoints) == maxCepsInCeb {
			continue
		}
		return cebBatch, nil
	}

	// Allocate a newCebBatch, if there is no ceb available in existing pool of cebs
	newCeb := NewCebBatch(client, "")
	// Start the go routine, to monitor for any changes in CEB and sync with APIserver.
	// TODO: Implement error handling here
	newCeb.RunUntil()
	return newCeb, nil
}

func queueCepInCeb(cep *cilium_v2.CiliumEndpoint, cebBatch *CebBatch) {
	// If cep already exists in ceb, compare new cep with cached cep.
	// Update only if there is any change.
	if cebName, ok := cacheCep[cep.GetName()]; ok {
		for i, ep := range cacheCeb[cebName].Ceb.Endpoints {
			if ep.GetName() == cep.GetName() {
				if cep.DeepEqual(&ep) {
					return
				}
				cacheCeb[cebName].Ceb.Endpoints =
					append(cacheCeb[cebName].Ceb.Endpoints[:i],
						cacheCeb[cebName].Ceb.Endpoints[i+1:]...)
				break
			}
		}
	}

	log.Infof("Queueing cep:%s into ceb:%s", cep.GetName(), cebBatch.Ceb.GetName())
	cacheCeb[cep.GetName()].Ceb.Endpoints =
		append(cacheCeb[cep.GetName()].Ceb.Endpoints, *cep)
	cacheCeb[cep.GetName()].syncState = syncStatePartial
	return
}

func RemoveCepFromCeb(cep *cilium_v2.CiliumEndpoint) error {
	// Check in local cache, if a given cep is already batched in one of cebs.
	// and if exists, delete cep from ceb.
	if cebName, ok := cacheCep[cep.GetName()]; ok {
		for i, ep := range cacheCeb[cebName].Ceb.Endpoints {
			if ep.GetName() == cep.GetName() {
				cacheCeb[cebName].Ceb.Endpoints =
					append(cacheCeb[cebName].Ceb.Endpoints[:i],
						cacheCeb[cebName].Ceb.Endpoints[i+1:]...)
				break
			}
		}
		log.Infof("Removed cep:%s from ceb:%s", cep.GetName(), cebName)
		if len(cacheCeb[cebName].Ceb.Endpoints) == 0 {
			close(cacheCeb[cebName].StopCh)
			return cacheCeb[cebName].DeleteCiliumEndpointBatch()
		}
	}
	cacheCeb[cep.GetName()].syncState = syncStatePartial

	return nil

}

func CiliumEndpointBatchSyncLocal(client clientset.CiliumV2Interface) {
	// List all CEB's from ApiServer
	var err error
	var cebs *cilium_v2.CiliumEndpointBatchList
	fmt.Println("Called CiliumEndpointBatchSyncLocal\n")
	log.Info("Called CiliumEndpointBatchSyncLocal\n")
	for i := 0; i < numRetries; i++ {
		cebs, err = client.CiliumEndpointBatches().List(context.Background(), meta_v1.ListOptions{})
		if err == nil {
			break
		}
		if err != nil && (errors.IsServerTimeout(err) || errors.IsTimeout(err)) {
			log.WithError(err).Infof("Failed to get Cilium Endpoint batch list from Apiserver, retry count :%d", i+1)
			continue
		}
	}
	// Nothing to process, return.
	if err != nil {
		log.WithError(err).Error("Multiple retries failed to get Cilium Endpoint batch list from Apiserver")
		return
	}
	// If there are no CEBs in datastore, nothing to be done.
	if len(cebs.Items) == 0 {
		log.Info("No CEB objects in datastore\n")
		return
	}

	for _, ceb := range cebs.Items {
		for _, cep := range ceb.Endpoints {
			cacheCep[cep.GetName()] = ceb.GetName()
			if _, ok := cacheCeb[ceb.GetName()]; !ok {
				cacheCeb[ceb.GetName()] = NewCebBatch(client, ceb.GetName())
			}
		}
	}
	// List all ceps from datastore, remove stale entries present in CEB
	// for example, some CEP entries are deleted in datastore, but it was
	// not yet updated in CEB.
	var ceps *cilium_v2.CiliumEndpointList
	for i := 0; i < numRetries; i++ {
		ceps, err = client.CiliumEndpoints("").List(context.Background(), meta_v1.ListOptions{})
		if err == nil {
			break
		}
		if err != nil && (errors.IsServerTimeout(err) || errors.IsTimeout(err)) {
			log.WithError(err).Infof("Failed to get Cilium Endpoint list from Apiserver, retry count :%d", i+1)
			continue
		}
	}
	// Nothing to process, return.
	if err != nil {
		log.WithError(err).Error("Multiple retries failed to get Cilium Endpoint list from Apiserver")
		return
	}

	actualCepList := make(map[string]*cilium_v2.CiliumEndpoint)
	for _, cep := range ceps.Items {
		actualCepList[cep.GetName()] = &cep
	}

	// Remove stale entries present in local cache
	for cepName, _ := range cacheCep {
		if _, ok := actualCepList[cepName]; !ok {
			RemoveCepFromCeb(actualCepList[cepName])
		}
	}

	return
}
