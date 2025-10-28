package cloudevent

import "time"

type Event struct {
	SpecVersion string    `json:"specversion"`
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Time        time.Time `json:"time"`
	Source      string    `json:"source"`

	DataContentType string `json:"datacontenttype,omitempty"`
	Data            []byte `json:"data,omitempty"`

	ResourceID        string     `json:"resourceid,omitempty"`
	ResourceVersion   *int64     `json:"resourceversion,omitempty"`
	DeletionTimestamp *time.Time `json:"deletiontimestamp,omitempty"`

	ClusterName    string `json:"clustername,omitempty"`
	OriginalSource string `json:"originalsource,omitempty"`
	SequenceID     string `json:"sequenceid,omitempty"`
	Metadata       string `json:"metadata,omitempty"`

	StatusHash string `json:"statushash,omitempty"`
}
