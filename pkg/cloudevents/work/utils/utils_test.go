package utils

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	workv1 "open-cluster-management.io/api/work/v1"
)

func TestPatch(t *testing.T) {
	cases := []struct {
		name      string
		patchType types.PatchType
		work      *workv1.ManifestWork
		patch     []byte
		validate  func(t *testing.T, work *workv1.ManifestWork)
	}{
		{
			name:      "json patch",
			patchType: types.JSONPatchType,
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
			},
			patch: []byte("[{\"op\":\"replace\",\"path\":\"/metadata/name\",\"value\":\"test1\"}]"),
			validate: func(t *testing.T, work *workv1.ManifestWork) {
				if work.Name != "test1" {
					t.Errorf("unexpected work %v", work)
				}
			},
		},
		{
			name:      "merge patch",
			patchType: types.MergePatchType,
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
			},
			patch: func() []byte {
				newWork := &workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test2",
						Namespace: "test2",
					},
				}
				data, err := json.Marshal(newWork)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}(),
			validate: func(t *testing.T, work *workv1.ManifestWork) {
				if work.Name != "test2" {
					t.Errorf("unexpected work %v", work)
				}
				if work.Namespace != "test2" {
					t.Errorf("unexpected work %v", work)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			work, err := Patch(c.patchType, c.work, c.patch)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			c.validate(t, work)
		})
	}
}

func TestUID(t *testing.T) {
	first := UID("source1", "ns", "name")
	second := UID("source1", "ns", "name")
	if first != second {
		t.Errorf("expected two uid equal, but %v, %v", first, second)
	}
}

func TestListWithOptions(t *testing.T) {
	cases := []struct {
		name          string
		works         []runtime.Object
		workNamespace string
		opts          metav1.ListOptions
		expectedWorks int
	}{
		{
			name: "list all works",
			works: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster2",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
			},
			workNamespace: metav1.NamespaceAll,
			opts:          metav1.ListOptions{},
			expectedWorks: 3,
		},
		{
			name: "list works from a given namespace",
			works: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster2",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
			},
			workNamespace: "cluster1",
			opts:          metav1.ListOptions{},
			expectedWorks: 2,
		},
		{
			name: "list with fields",
			works: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "false",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster2",
						Labels: map[string]string{
							"test": "false",
						},
					},
				},
			},
			opts: metav1.ListOptions{
				FieldSelector: "metadata.name=t1",
			},
			workNamespace: "cluster1",
			expectedWorks: 1,
		},
		{
			name: "list with labels",
			works: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "false",
						},
					},
				},
			},
			opts: metav1.ListOptions{
				LabelSelector: "test=true",
			},
			workNamespace: "cluster1",
			expectedWorks: 1,
		},
		{
			name: "list with labels and fields",
			works: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t1",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "true",
						},
					},
				},
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "t2",
						Namespace: "cluster1",
						Labels: map[string]string{
							"test": "false",
						},
					},
				},
			},
			opts: metav1.ListOptions{
				LabelSelector: "test=true",
				FieldSelector: "metadata.name=t1",
			},
			workNamespace: "cluster1",
			expectedWorks: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := cache.NewStore(cache.MetaNamespaceKeyFunc)
			for _, work := range c.works {
				if err := store.Add(work); err != nil {
					t.Fatal(err)
				}
			}
			works, err := ListWorksWithOptions(store, c.workNamespace, c.opts)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			if len(works) != c.expectedWorks {
				t.Errorf("expected %d, but %v", c.expectedWorks, works)
			}

		})
	}
}
