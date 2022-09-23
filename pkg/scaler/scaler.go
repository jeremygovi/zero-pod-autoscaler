// Copyright 2020 GreenKey Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License.  You
// may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
package scaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var KeyScaleDownAt = "zero-pod-autoscaler/scale-down-at"

type Scaler struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string

	// Target address to which requests are proxied. We need this
	// because endpoints reporting available doesn't mean the
	// Service can actually handle a request... so we ping the
	// actual target also.
	Target string

	TTL              time.Duration
	availableRequest chan chan chan struct{}
	scaleUp          chan int
	connectionInc    chan int

	updated chan interface{}
	deleted chan interface{}
}

func New(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	deployOptions, epOptions metav1.ListOptions,
	target string,
	ttl time.Duration,
) (*Scaler, error) {
	var deploy appsv1.Deployment
	var ep corev1.Endpoints

	log.Printf("INFO: Watching namespace %s", namespace)
	if list, err := client.AppsV1().Deployments(namespace).List(deployOptions); err != nil {
		return nil, err
	} else {
		if len(list.Items) > 1 {
			return nil, fmt.Errorf("ERROR: matched more than 1 Deployment (%d)", len(list.Items))
		}

		if len(list.Items) == 0 {
			return nil, fmt.Errorf("ERROR: 0 Deployment found")
		}

		deploy = list.Items[0]
	}

	if list, err := client.CoreV1().Endpoints(namespace).List(epOptions); err != nil {
		if err != nil {
			return nil, err
		}
	} else {
		if len(list.Items) > 1 {
			return nil, fmt.Errorf("ERROR: matched more than 1 Endpoint (%d)", len(list.Items))
		}

		if len(list.Items) == 0 {
			return nil, fmt.Errorf("ERROR: 0 Endpoint found")
		}

		ep = list.Items[0]
	}

	log.Printf("INFO: Watching %s/%s", "Endpoints", ep.Name)
	log.Printf("INFO: Watching %s/%s", "Target", target)
	log.Printf("INFO: Watching %s/%s", "Deployment", deploy.Name)

	fieldSelector := fmt.Sprintf("metadata.name=%s", deploy.Name)

	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		1*time.Minute,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fieldSelector
		}))

	updated := make(chan interface{})
	deleted := make(chan interface{})
	availableRequest := make(chan chan chan struct{})
	scaleUp := make(chan int)
	connectionInc := make(chan int)

	funcs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			updated <- obj
		},
		UpdateFunc: func(oldObj, obj interface{}) {
			updated <- obj
		},
		DeleteFunc: func(obj interface{}) {
			deleted <- obj
		},
	}

	if informer := factory.Apps().V1().Deployments().Informer(); true {
		informer.AddEventHandler(funcs)
		go informer.Run(ctx.Done())
	}

	if informer := factory.Core().V1().Endpoints().Informer(); true {
		informer.AddEventHandler(funcs)
		go informer.Run(ctx.Done())
	}

	sc := &Scaler{
		Client:           client,
		Namespace:        namespace,
		Name:             deploy.Name,
		Target:           target,
		TTL:              ttl,
		availableRequest: availableRequest,
		scaleUp:          scaleUp,
		connectionInc:    connectionInc,
		updated:          updated,
		deleted:          deleted,
	}

	return sc, nil
}

func (sc *Scaler) TryConnect(ctx context.Context) error {
	log.Printf("DEBUG: entering scaler.TryConnect function")
	dialer := net.Dialer{}
	timeout_ms := 500.0
	factor := 2.0 // timeout series: 500, 1s, 2s, 4s...
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		subctx, _ := context.WithTimeout(ctx, time.Duration(timeout_ms)*time.Millisecond)
		timeout_ms *= factor
		log.Printf("DEBUG: starting test connection to %s. subctx: %v", sc.Target, subctx)
		conn, err := dialer.DialContext(subctx, "tcp", sc.Target)
		if err != nil {
			log.Printf("ERROR: failed test connection to %s: %v", sc.Target, err)
			continue
		}

		conn.Close()
		return nil
	}
}

func (sc *Scaler) Run(ctx context.Context) error {
	log.Printf("DEBUG: entering scaler.Run function")
	replicas := int32(-1)
	readyAddresses := -1
	notReadyAddresses := -1
	connCount := 0

	// channel is closed when upstream is available
	var available chan struct{}
	closedChan := make(chan struct{})
	close(closedChan)

	resourceVersion := ""

	scaleDownAt := time.Now().Add(sc.TTL)

	for {
		select {
		case <-ctx.Done():
			log.Printf("DEBUG: ctx.done")
			return fmt.Errorf("%v", ctx.Err())
		case i := <-sc.connectionInc:
			log.Printf("DEBUG: sc.connectionInc")
			connCount += i
		case obj := <-sc.updated:
			//log.Printf("DEBUG: sc.updated") -> passe tres souvent dedans
			switch resource := obj.(type) {
			case *corev1.Endpoints:
				log.Printf("DEBUG: corev1.Endpoints")
				r := 0
				nr := 0

				for _, subset := range resource.Subsets {
					r += len(subset.Addresses)
					nr += len(subset.NotReadyAddresses)
				}

				if r != readyAddresses || nr != notReadyAddresses {
					log.Printf("INFO: %s/%s: readyAddresses=%d notReadyAddresses=%d",
						"Endpoints", resource.Name, r, nr)
				}

				readyAddresses, notReadyAddresses = r, nr

				if readyAddresses == 0 {
					continue
				}

				// nothing is waiting
				if available == nil {
					continue
				}

				log.Printf("INFO: %s/%s has ready addresses; confirming can connect to %s",
					"Endpoints", resource.Name, sc.Target)

				subctx, _ := context.WithTimeout(ctx, 15*time.Second)
				if err := sc.TryConnect(subctx); err != nil {
					log.Fatalf("ERROR: %s/%s has ready addresses but failed to connect to %s: %v",
						"Endpoints", resource.Name, sc.Target, err)
				}

				log.Printf("INFO: %s available; notifying waiters", resource.Name)
				close(available)
				available = nil
			case *appsv1.Deployment:
				//log.Printf("DEBUG: appsv1.Deployment") -> passe tres souvent dans ce cas
				resourceVersion = resource.ResourceVersion

				if timestamp, ok := resource.Annotations[KeyScaleDownAt]; ok {
					if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
						scaleDownAt = t
					}
				}

				if resource.Spec.Replicas != nil {
					if replicas != *resource.Spec.Replicas {
						log.Printf("INFO: Received notification from kubernetes API: %s/%s: replicas: %d",
							"Deployment", resource.Name, *resource.Spec.Replicas)
					}
					replicas = *resource.Spec.Replicas
				}
			}
		case obj := <-sc.deleted:
			log.Printf("DEBUG: sc.deleted")
			switch resource := obj.(type) {
			case *corev1.Endpoints:
				log.Fatalf("INFO: Received notification from kubernetes API: %s/%s: deleted", "Endpoints", resource.Name)
			case *appsv1.Deployment:
				log.Fatalf("INFO: Received notification from kubernetes API: %s/%s: deleted", "Deployment", resource.Name)
			}
		case reply := <-sc.availableRequest:
			log.Printf("DEBUG: reply := <-sc.availableRequest")
			// set time to scale down
			sc.extendScaleDownAtMaybe(scaleDownAt)

			if readyAddresses > 0 {
				log.Printf("DEBUG: reply readyAddresses > 0")
				// is currently available; send the already-closed channel
				reply <- closedChan
				continue
			}

			// nothing ready, reply with channel that gets closed when ready
			if available == nil {
				log.Printf("DEBUG: reply available == nil")
				available = make(chan struct{})
			}
			reply <- available

			if replicas == 0 {
				go func() { sc.scaleUp <- 0 }()
			}
		case attemptNumber := <-sc.scaleUp:
			log.Printf("DEBUG: attemptNumber := <-sc.scaleUp")
			if replicas == 0 {
				if err := sc.updateScale(resourceVersion, 1); err != nil {
					log.Printf("ERROR: %s/%s: failed to scale up: %v %T",
						"Deployment", sc.Name, err, err)
					// try again; usual error is that resource is out of date
					// TODO: try again ONLY when error is that resource is out of date
					if attemptNumber < 10 {
						go func() { sc.scaleUp <- attemptNumber + 1 }()
					}
				}
			}
		case <-time.After(1 * time.Second):
			// log.Printf("DEBUG: case <-time.After(1 * time.Second)")
			if connCount > 0 {
				sc.extendScaleDownAtMaybe(scaleDownAt)
			}

			if connCount == 0 && replicas > 0 && time.Now().After(scaleDownAt) {
				log.Printf("INFO: %s/%s: scaling down after %s: replicas=%d connections=%d",
					"Deployment", sc.Name, sc.TTL, replicas, connCount)

				if err := sc.updateScale(resourceVersion, 0); err != nil {
					log.Printf("ERROR: %s/%s: failed to scale to zero: %v",
						"Deployment", sc.Name, err)
				}
			}
		}
	}
}

func (sc *Scaler) extendScaleDownAtMaybe(scaleDownAt time.Time) {
	if !time.Now().After(scaleDownAt.Add(sc.TTL / -2)) {
		return
	}

	path := fmt.Sprintf("/metadata/annotations/%s", JsonPatchEscape(KeyScaleDownAt))

	patch := []map[string]string{
		{
			"op":    "replace",
			"path":  path,
			"value": time.Now().Add(sc.TTL).Format(time.RFC3339),
		},
	}

	body, err := json.Marshal(patch)
	if err != nil {
		log.Printf("ERROR: failed to marshal patch to json: %v", err)
	}

	if _, err := sc.Client.AppsV1().Deployments(sc.Namespace).
		Patch(sc.Name, types.JSONPatchType, body); err != nil {
		log.Printf("ERROR: %s/%s: failed to patch: %v",
			"Deployment", sc.Name, err)
	}

	log.Printf("INFO: %s/%s: updated scaleDownAt to %s from now",
		"Deployment", sc.Name, sc.TTL)
}

func (sc *Scaler) updateScale(resourceVersion string, replicas int32) error {
	deployments := sc.Client.AppsV1().Deployments(sc.Namespace)

	scale := autoscalingv1.Scale{}
	scale.Namespace = sc.Namespace
	scale.Name = sc.Name
	scale.ResourceVersion = resourceVersion

	scale.Spec.Replicas = replicas

	if _, err := deployments.UpdateScale(sc.Name, &scale); err != nil {
		return err
	}

	log.Printf("INFO: %s/%s: scaled to %d", "Deployment", sc.Name, replicas)

	return nil
}

func (sc *Scaler) UseConnection(f func() error) error {
	log.Printf("DEBUG: entering scaler.UseConnection function")
	sc.connectionInc <- 1
	err := f()
	sc.connectionInc <- -1
	return err
}

// Available returns a channel that will be closed when upstream is
// available. The returned channel may already be closed if upstream
// is currently available.
func (sc *Scaler) Available() (available chan struct{}) {
	log.Printf("DEBUG: entering scaler.Available function")

	reply := make(chan chan struct{})

	log.Printf("DEBUG: reply: %v", reply)
	sc.availableRequest <- reply
	return <-reply
}
