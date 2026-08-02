/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/util/routine"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// Port of the refill branch's drain-to-empty benchmark, with the arm gate
// switched to FairSharingDeepAdmission, so the look-ahead prototype can be
// compared against the same fixtures. Cycle counts are deterministic and
// comparable across branches; time-based metrics are only comparable within
// this branch (off vs on).

type drainScenario struct {
	name          string
	clusterQueues []kueue.ClusterQueue
	localQueues   []kueue.LocalQueue
	workloads     []kueue.Workload
}

func drainScenarios() []*drainScenario {
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	seq := 0
	// Second-spaced unique timestamps: metav1.Time is second-granular once
	// objects round trip, and sub-second spacing collapses into FIFO ties.
	nextCreation := func() time.Time {
		seq++
		return base.Add(time.Duration(seq) * time.Second)
	}
	pending := func(name, lq string) kueue.Workload {
		return *utiltestingapi.MakeWorkload(name, "default").
			Queue(kueue.LocalQueueName(lq)).
			Creation(nextCreation()).
			PodSets(*utiltestingapi.MakePodSet("one", 1).
				Request(corev1.ResourceCPU, "1").
				Obj()).
			Obj()
	}
	cq := func(name, cohort, nominal, borrow string) kueue.ClusterQueue {
		return *utiltestingapi.MakeClusterQueue(name).
			Cohort(kueue.CohortReference(cohort)).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, nominal, borrow).Obj()).
			Obj()
	}
	lq := func(cqName string) kueue.LocalQueue {
		return *utiltestingapi.MakeLocalQueue(cqName+"-lq", "default").
			ClusterQueue(cqName).Obj()
	}

	backlog := &drainScenario{name: "backlog"}
	backlog.clusterQueues = append(backlog.clusterQueues, cq("drain-backlog", "drain", "4", "28"))
	backlog.localQueues = append(backlog.localQueues, lq("drain-backlog"))
	for i := range 32 {
		backlog.workloads = append(backlog.workloads, pending(fmt.Sprintf("backlog-%02d", i), "drain-backlog-lq"))
	}
	for q := range 7 {
		name := fmt.Sprintf("drain-steady-%d", q)
		backlog.clusterQueues = append(backlog.clusterQueues, cq(name, "drain", "6", "0"))
		backlog.localQueues = append(backlog.localQueues, lq(name))
		for i := range 2 {
			backlog.workloads = append(backlog.workloads, pending(fmt.Sprintf("steady-%d-%d", q, i), name+"-lq"))
		}
	}

	balanced := &drainScenario{name: "balanced"}
	for q := range 8 {
		name := fmt.Sprintf("drain-bal-%d", q)
		balanced.clusterQueues = append(balanced.clusterQueues, cq(name, "drain", "8", "0"))
		balanced.localQueues = append(balanced.localQueues, lq(name))
		for i := range 8 {
			balanced.workloads = append(balanced.workloads, pending(fmt.Sprintf("bal-%d-%d", q, i), name+"-lq"))
		}
	}

	return []*drainScenario{backlog, balanced}
}

func setupDrain(tb testing.TB, sc *drainScenario) (context.Context, *qcache.Manager, *Scheduler, *sync.WaitGroup) {
	tb.Helper()
	log := logr.Discard()
	ctx := logr.NewContext(tb.Context(), log)
	cl := utiltesting.NewClientBuilder().
		WithObjects(utiltesting.MakeNamespaceWrapper("default").Obj()).
		WithLists(
			&kueue.WorkloadList{Items: sc.workloads},
			&kueue.LocalQueueList{Items: sc.localQueues},
			&kueue.ClusterQueueList{Items: sc.clusterQueues},
		).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, client client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				return nil
			},
		}).
		Build()
	recorder := &utiltesting.EventRecorder{}
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithFairSharing(true))
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	for i := range sc.localQueues {
		if err := qManager.AddLocalQueue(ctx, sc.localQueues[i].DeepCopy()); err != nil {
			tb.Fatalf("Failed adding LocalQueue %s: %v", sc.localQueues[i].Name, err)
		}
	}
	for i := range sc.clusterQueues {
		cqCopy := sc.clusterQueues[i].DeepCopy()
		if err := cqCache.AddClusterQueue(ctx, cqCopy); err != nil {
			tb.Fatalf("Failed adding ClusterQueue %s to cache: %v", cqCopy.Name, err)
		}
		if err := qManager.AddClusterQueue(ctx, cqCopy); err != nil {
			tb.Fatalf("Failed adding ClusterQueue %s to manager: %v", cqCopy.Name, err)
		}
	}
	scheduler := New(qManager, cqCache, cl, recorder,
		WithFairSharing(&config.FairSharing{}),
		WithPreemptionExpectations(preemptexpectations.New()))
	wg := &sync.WaitGroup{}
	scheduler.setAdmissionRoutineWrapper(routine.NewWrapper(
		func() { wg.Add(1) },
		func() { wg.Done() },
	))
	return ctx, qManager, scheduler, wg
}

func drainToEmpty(ctx context.Context, tb testing.TB, qManager *qcache.Manager, scheduler *Scheduler, wg *sync.WaitGroup, maxCycles int, afterCycle func(cycle int)) int {
	tb.Helper()
	b, isBench := tb.(*testing.B)
	cycles := 0
	for {
		if isBench {
			b.StopTimer()
		}
		drained := qManager.Dump() == nil
		if drained {
			if inadmissible := qManager.DumpInadmissible(); inadmissible != nil {
				tb.Fatalf("workloads left inadmissible after drain: %v", inadmissible)
			}
		} else if cycles >= maxCycles {
			tb.Fatalf("queues did not drain within %d cycles; left: %v, inadmissible: %v",
				maxCycles, qManager.Dump(), qManager.DumpInadmissible())
		}
		if isBench {
			b.StartTimer()
		}
		if drained {
			return cycles
		}
		scheduler.schedule(ctx)
		wg.Wait()
		cycles++
		if afterCycle != nil {
			afterCycle(cycles)
		}
	}
}

func BenchmarkSchedulerFairSharingLookAheadDrain(b *testing.B) {
	for _, sc := range drainScenarios() {
		for _, arm := range []struct {
			name   string
			gateOn bool
		}{
			{name: "off", gateOn: false},
			{name: "on", gateOn: true},
		} {
			b.Run(fmt.Sprintf("scenario=%s/gate=%s", sc.name, arm.name), func(b *testing.B) {
				features.SetFeatureGateDuringTest(b, features.FairSharingDeepAdmission, arm.gateOn)
				totalWorkloads := len(sc.workloads)
				totalCycles := 0
				iterations := 0
				for b.Loop() {
					b.StopTimer()
					ctx, qManager, scheduler, wg := setupDrain(b, sc)
					runtime.GC()
					b.StartTimer()
					totalCycles += drainToEmpty(ctx, b, qManager, scheduler, wg, 4*totalWorkloads+8, nil)
					iterations++
				}
				b.ReportMetric(float64(totalCycles)/float64(iterations), "cycles/op")
				b.ReportMetric(float64(totalWorkloads*iterations)/float64(totalCycles), "admits/cycle")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(totalWorkloads*iterations), "ns/admit")
			})
		}
	}
}

// TestLookAheadDrainScenarios verifies both arms drain fully and records the
// deterministic cycle counts and per-cycle admissions for comparison with the
// refill branch.
func TestLookAheadDrainScenarios(t *testing.T) {
	total := func(m map[kueue.ClusterQueueReference][]workload.Reference) int {
		n := 0
		for _, r := range m {
			n += len(r)
		}
		return n
	}
	for _, sc := range drainScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			for _, gateOn := range []bool{false, true} {
				features.SetFeatureGateDuringTest(t, features.FairSharingDeepAdmission, gateOn)
				ctx, qManager, scheduler, wg := setupDrain(t, sc)
				prev := qManager.Dump()
				afterCycle := func(c int) {
					cur := qManager.Dump()
					t.Logf("gate=%v cycle %d: admitted %d", gateOn, c, total(prev)-total(cur))
					prev = cur
				}
				cycles := drainToEmpty(ctx, t, qManager, scheduler, wg, 4*len(sc.workloads)+8, afterCycle)
				t.Logf("gate=%v: %d workloads drained in %d cycles", gateOn, len(sc.workloads), cycles)
			}
		})
	}
}
