# Cloudevents Clients

We have implemented the [cloudevents](https://cloudevents.io/)-based clients in this package to assist developers in
easily implementing the [Event Based Manifestwork](https://github.com/open-cluster-management-io/enhancements/tree/main/enhancements/sig-architecture/224-event-based-manifestwork)
proposal.

## Generic Clients

The generic client (`generic.CloudEventsClient`) is used to resync/publish/subscribe resource objects between sources and agents with cloudevents.

A resource object can be any object that implements the `generic.ResourceObject` interface.

### Building a generic client on a source

To build a generic client on the source, developers use the `options.GenericClientOptions` along with a protocol configuration.

The client creation requires:

1. A protocol configuration (one of):
    - `mqtt.MQTTOptions` - MQTT protocol configuration
    - `grpc.GRPCOptions` - gRPC protocol configuration
    - `pubsub.PubSubOptions` - Google Cloud Pub/Sub protocol configuration

2. A codec (`generic.Codec`) to encode/decode resource objects to/from CloudEvents.

3. A client ID - unique identifier for the client instance.

4. A source ID - unique identifier for the source (e.g., hub controller name or service name).

5. A watcher store (`store.ClientWatcherStore`) to cache resources and provide watch capability.

Example using MQTT protocol with ManifestBundle codec:

```golang
import (
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/options"
    sourcecodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/source/codec"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

// Load MQTT configuration
mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags("path/to/mqtt-config.yaml")
if err != nil {
    return err
}

sourceID := "my-controller"
clientID := fmt.Sprintf("%s-client", sourceID)

// Create generic client options
opt := options.NewGenericClientOptions(mqttOptions, sourcecodec.NewManifestBundleCodec(), clientID).
    WithSourceID(sourceID).
    WithClientWatcherStore(myWatcherStore)

// Build the source client (this also handles subscription and resync automatically)
cloudEventsClient, err := opt.SourceClient(ctx)
if err != nil {
    return err
}
```

The `SourceClient` method automatically:
- Subscribes to receive resource status from agents
- Sends resync requests after subscription is established
- Handles reconnection and resyncing

### Building a generic client on a managed cluster

To build a generic client on a managed cluster (agent), developers use the `options.GenericClientOptions` along with a protocol configuration.

The client creation requires:

1. A protocol configuration (one of):
    - `mqtt.MQTTOptions` - MQTT protocol configuration
    - `grpc.GRPCOptions` - gRPC protocol configuration
    - `pubsub.PubSubOptions` - Google Cloud Pub/Sub protocol configuration

2. A codec (`generic.Codec`) to encode/decode resource objects to/from CloudEvents.

3. A client ID - unique identifier for the client instance.

4. A cluster name - the name of the managed cluster on which the agent runs.

5. A watcher store (`store.ClientWatcherStore`) to cache resources and provide watch capability (optional for agent, defaults to `AgentInformerWatcherStore` if not provided).

Example using MQTT protocol with ManifestBundle codec:

```golang
import (
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/options"
    agentcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/agent/codec"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

// Load MQTT configuration
mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags("path/to/mqtt-config.yaml")
if err != nil {
    return err
}

clusterName := "cluster1"
clientID := fmt.Sprintf("%s-agent", clusterName)

// Create generic client options
opt := options.NewGenericClientOptions(mqttOptions, agentcodec.NewManifestBundleCodec(), clientID).
    WithClusterName(clusterName).
    WithClientWatcherStore(myWatcherStore)

// Build the agent client (this also handles subscription and resync automatically)
cloudEventsClient, err := opt.AgentClient(ctx)
if err != nil {
    return err
}
```

The `AgentClient` method automatically:
- Subscribes to receive resource specs from sources
- Sends resync requests after subscription is established
- Handles reconnection and resyncing

### Advanced Options

Both `SourceClient` and `AgentClient` support additional configuration:

```golang
opt := options.NewGenericClientOptions(config, codec, clientID).
    WithClientWatcherStore(watcherStore).
    WithSourceID(sourceID).  // or WithClusterName(clusterName) for agent
    WithSubscription(false). // Disable automatic subscription (default: true)
    WithResyncEnabled(false) // Disable automatic resync (default: true)
```

## Supported Protocols and Drivers

The CloudEvents clients support the following protocols/drivers:

- [MQTT Protocol/Driver](./generic/options/mqtt)
- [gRPC Protocol/Driver](./generic/options/grpc)
- [Google Cloud Pub/Sub Protocol/Driver](./generic/options/v2/pubsub)

To create CloudEvents source/agent options for these supported protocols/drivers, developers need to provide configuration specific to the protocol/driver. The configuration format resembles the kubeconfig for the Kubernetes client-go but has a different schema.

### MQTT Protocol/Driver

Below is an example of a YAML configuration for the MQTT protocol:

```yaml
broker: broker.example.com:1883
username: maestro
password: password
topics:
  sourceEvents: sources/maestro/consumers/+/sourceevents
  agentEvents: $share/statussubscribers/sources/maestro/consumers/+/agentevents
```

For detailed configuration options for the MQTT driver, refer to the [MQTT driver options package](./generic/options/mqtt/options.go).

### gRPC Protocol/Driver

Here's an example of a YAML configuration for the gRPC protocol:

```yaml
url: grpc.example.com:8443
caFile: /certs/ca.crt
clientCertFile: /certs/client.crt
clientKeyFile: /certs/client.key
```

For detailed configuration options for the gRPC driver, refer to the [gRPC driver options package](./generic/options/grpc/options.go).

### Google Cloud Pub/Sub Protocol/Driver

Here's an example of a YAML configuration for the Google Cloud Pub/Sub protocol for a source:

```yaml
projectID: my-project
endpoint: https://pubsub.us-east1.googleapis.com # optional, leave empty for global, or set a regional URL.
credentialsFile: /path/to/credentials.json
topics:
  sourceEvents: projects/my-project/topics/sourceevents
  sourceBroadcast: projects/my-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/my-project/subscriptions/agentevents-source1
  agentBroadcast: projects/my-project/subscriptions/agentbroadcast-source1
```

And here's an example configuration for an agent:

```yaml
projectID: my-project
endpoint: https://pubsub.us-east1.googleapis.com # optional, leave empty for global, or set a regional URL.
credentialsFile: /path/to/credentials.json
topics:
  agentEvents: projects/my-project/topics/agentevents
  agentBroadcast: projects/my-project/topics/agentbroadcast
subscriptions:
  sourceEvents: projects/my-project/subscriptions/sourceevents-cluster1
  sourceBroadcast: projects/my-project/subscriptions/sourcebroadcast-cluster1
```

**Note**: The Pub/Sub protocol uses separate topics and subscriptions for different event types:
- **Source** uses `sourceEvents`/`sourceBroadcast` topics to publish events and `agentEvents`/`agentBroadcast` subscriptions to receive events from agents
- **Agent** uses `agentEvents`/`agentBroadcast` topics to publish events and `sourceEvents`/`sourceBroadcast` subscriptions to receive events from sources
- Both `sourceBroadcast` and `agentBroadcast` channels are used for resync requests
- **All topics and subscriptions must be created before running the source and agent**
- The subscription for `agentEvents` is a filtered subscription with filter `attributes."ce-clustername"="<clustername>"`
- The subscription for `sourceEvents` is a filtered subscription with filter `attributes."ce-originalsource"="<sourceID>"`
- The subscriptions for `agentBroadcast` and `sourceBroadcast` are subscriptions without filters, allowing broadcast messages to reach all subscribers for resync events.

For detailed configuration options for the Pub/Sub driver, refer to the [Pub/Sub driver options package](./generic/options/v2/pubsub).

## Work Clients

The Work clients provide Kubernetes client-go compatible interfaces for working with `ManifestWork` resources via CloudEvents.

### Building a ManifestWorkSourceClient on the hub cluster with SourceInformerWatcherStore

The `SourceInformerWatcherStore` integrates with Kubernetes informers to watch and cache works.

Example using MQTT protocol with ManifestBundle codec:

```golang
import (
    workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/options"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work"
    sourcecodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/source/codec"
    workstore "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/store"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

sourceID := "example-controller"
clientID := fmt.Sprintf("%s-client", sourceID)

// Building the clients based on cloudevents with MQTT
mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags("path/to/mqtt-config.yaml")
if err != nil {
    return err
}

// New a SourceInformerWatcherStore
watcherStore := workstore.NewSourceInformerWatcherStore(ctx)

// Create generic client options and build source client holder
opt := options.NewGenericClientOptions(mqttOptions, sourcecodec.NewManifestBundleCodec(), clientID).
    WithClientWatcherStore(watcherStore).
    WithSourceID(sourceID)

clientHolder, err := work.NewSourceClientHolder(ctx, opt)
if err != nil {
    return err
}

factory := workinformers.NewSharedInformerFactoryWithOptions(clientHolder.WorkInterface(), 5*time.Minute)
informer := factory.Work().V1().ManifestWorks()

// Use the informer's store as the SourceInformerWatcherStore's store
watcherStore.SetInformer(informer.Informer())

// Building controllers with ManifestWork informer ...

go informer.Informer().Run(ctx.Done())

// Use the manifestWorkClient to create/update/delete manifestworks
```

### Building a ManifestWorkAgentClient on the managed cluster with AgentInformerWatcherStore

The `AgentInformerWatcherStore` is used for building `ManifestWork` agent clients on managed clusters.

Example using MQTT protocol with ManifestBundle codec:

```golang
import (
    "time"

    workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/options"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work"
    agentcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/agent/codec"
    workstore "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/store"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

clusterName := "cluster1"
clientID := fmt.Sprintf("%s-work-agent", clusterName)

// Building the clients based on cloudevents with MQTT
mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags("path/to/mqtt-config.yaml")
if err != nil {
    return err
}

// New an AgentInformerWatcherStore
watcherStore := workstore.NewAgentInformerWatcherStore()

// Create generic client options and build agent client holder
opt := options.NewGenericClientOptions(mqttOptions, agentcodec.NewManifestBundleCodec(), clientID).
    WithClientWatcherStore(watcherStore).
    WithClusterName(clusterName)

clientHolder, err := work.NewAgentClientHolder(ctx, opt)
if err != nil {
    return err
}

manifestWorkClient := clientHolder.ManifestWorks(clusterName)

factory := workinformers.NewSharedInformerFactoryWithOptions(
    clientHolder.WorkInterface(),
    5*time.Minute,
    workinformers.WithNamespace(clusterName),
)
informer := factory.Work().V1().ManifestWorks()

// Building controllers with ManifestWork informer ...

// Start the ManifestWork informer
go informer.Informer().Run(ctx.Done())

// Use the manifestWorkClient to update work status
```

### Building garbage collector for work controllers on the hub cluster

The garbage collector is an optional component running alongside the `ManifestWork` source client. It is used to clean up `ManifestWork` resources owned by other resources. For example, in the addon-framework, `ManifestWork` resources created by addon controllers have owner references pointing to `ManagedClusterAddon` resources, and when the owner resources are deleted, the `ManifestWork` resources should be deleted as well.

Developers need to provide the following to build and run the garbage collector:
1. Client Holder (`work.ClientHolder`): This contains the `ManifestWork` client and informer, which can be built using the builder mentioned in the previous sections.
2. Metadata Client (`metadata.Interface`): This is used to retrieve the owner resources of the `ManifestWork` resources.
3. Owner resource filters map (`map[schema.GroupVersionResource]*metav1.ListOptions`): This is used to filter the owner resources of the `ManifestWork` resources.

```golang
import (
    "k8s.io/apimachinery/pkg/runtime/schema"
    addonv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
    "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/garbagecollector"
)

listOptions := &metav1.ListOptions{
    FieldSelector: fmt.Sprintf("metadata.name=%s", addonName),
}
ownerGVRFilters := map[schema.GroupVersionResource]*metav1.ListOptions{
    // ManagedClusterAddon is the owner of the ManifestWork resources filtered by the addon name
    addonv1alpha1.SchemeGroupVersion.WithResource("managedclusteraddons"): listOptions,
}

// Initialize the garbage collector
garbageCollector := garbagecollector.NewGarbageCollector(clientHolder, workInformer, metadataClient, ownerGVRFilters)

// Run the garbage collector with 1 worker to handle the garbage collection
go garbageCollector.Run(ctx, 1)
```

## Additional Resource Clients

Beyond `ManifestWork`, this package also provides cloudevents-based clients for other OCM resources:

- **ManagedClusterAddOn Clients** (`clients/addon`): For managing ManagedClusterAddOn via cloudevents on the managed cluster
- **ManagedCluster Client** (`clients/cluster`): For managing ManagedCluster via cloudevents on the managed cluster
- **CertificateSigningRequest Client** (`clients/csr`): For managing CSRs via cloudevents on the managed cluster
- **Lease Client** (`clients/lease`): For managing Lease resources on the managed cluster
- **ServiceAccount Client** (`clients/serviceaccount`): For managing ServiceAccount resources on the managed cluster
- **Event Client** (`clients/event`): For managing Kubernetes Event resources on the managed cluster

Each client follows the same pattern as the Work client, using `GenericClientOptions` with appropriate codecs and stores.
