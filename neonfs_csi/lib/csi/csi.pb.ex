defmodule Csi.V1.PluginCapability.Service.Type do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.PluginCapability.Service.Type",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:CONTROLLER_SERVICE, 1)
  field(:VOLUME_ACCESSIBILITY_CONSTRAINTS, 2)
  field(:GROUP_CONTROLLER_SERVICE, 3)
end

defmodule Csi.V1.PluginCapability.VolumeExpansion.Type do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.PluginCapability.VolumeExpansion.Type",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:ONLINE, 1)
  field(:OFFLINE, 2)
end

defmodule Csi.V1.VolumeCapability.AccessMode.Mode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.VolumeCapability.AccessMode.Mode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:SINGLE_NODE_WRITER, 1)
  field(:SINGLE_NODE_READER_ONLY, 2)
  field(:MULTI_NODE_READER_ONLY, 3)
  field(:MULTI_NODE_SINGLE_WRITER, 4)
  field(:MULTI_NODE_MULTI_WRITER, 5)
  field(:SINGLE_NODE_SINGLE_WRITER, 6)
  field(:SINGLE_NODE_MULTI_WRITER, 7)
end

defmodule Csi.V1.ControllerServiceCapability.RPC.Type do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.ControllerServiceCapability.RPC.Type",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:CREATE_DELETE_VOLUME, 1)
  field(:PUBLISH_UNPUBLISH_VOLUME, 2)
  field(:LIST_VOLUMES, 3)
  field(:GET_CAPACITY, 4)
  field(:CREATE_DELETE_SNAPSHOT, 5)
  field(:LIST_SNAPSHOTS, 6)
  field(:CLONE_VOLUME, 7)
  field(:PUBLISH_READONLY, 8)
  field(:EXPAND_VOLUME, 9)
  field(:LIST_VOLUMES_PUBLISHED_NODES, 10)
  field(:VOLUME_CONDITION, 11)
  field(:GET_VOLUME, 12)
  field(:SINGLE_NODE_MULTI_WRITER, 13)
  field(:MODIFY_VOLUME, 14)
end

defmodule Csi.V1.VolumeUsage.Unit do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.VolumeUsage.Unit",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:BYTES, 1)
  field(:INODES, 2)
end

defmodule Csi.V1.NodeServiceCapability.RPC.Type do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.NodeServiceCapability.RPC.Type",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:STAGE_UNSTAGE_VOLUME, 1)
  field(:GET_VOLUME_STATS, 2)
  field(:EXPAND_VOLUME, 3)
  field(:VOLUME_CONDITION, 4)
  field(:SINGLE_NODE_MULTI_WRITER, 5)
  field(:VOLUME_MOUNT_GROUP, 6)
end

defmodule Csi.V1.GroupControllerServiceCapability.RPC.Type do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "csi.v1.GroupControllerServiceCapability.RPC.Type",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:UNKNOWN, 0)
  field(:CREATE_DELETE_GET_VOLUME_GROUP_SNAPSHOT, 1)
end

defmodule Csi.V1.GetPluginInfoRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetPluginInfoRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.GetPluginInfoResponse.ManifestEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetPluginInfoResponse.ManifestEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.GetPluginInfoResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetPluginInfoResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:vendor_version, 2, type: :string, json_name: "vendorVersion")
  field(:manifest, 3, repeated: true, type: Csi.V1.GetPluginInfoResponse.ManifestEntry, map: true)
end

defmodule Csi.V1.GetPluginCapabilitiesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetPluginCapabilitiesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.GetPluginCapabilitiesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetPluginCapabilitiesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capabilities, 1, repeated: true, type: Csi.V1.PluginCapability)
end

defmodule Csi.V1.PluginCapability.Service do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.PluginCapability.Service",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: Csi.V1.PluginCapability.Service.Type, enum: true)
end

defmodule Csi.V1.PluginCapability.VolumeExpansion do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.PluginCapability.VolumeExpansion",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: Csi.V1.PluginCapability.VolumeExpansion.Type, enum: true)
end

defmodule Csi.V1.PluginCapability do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.PluginCapability",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:service, 1, type: Csi.V1.PluginCapability.Service, oneof: 0)

  field(:volume_expansion, 2,
    type: Csi.V1.PluginCapability.VolumeExpansion,
    json_name: "volumeExpansion",
    oneof: 0
  )
end

defmodule Csi.V1.ProbeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ProbeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.ProbeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ProbeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ready, 1, type: Google.Protobuf.BoolValue)
end

defmodule Csi.V1.CreateVolumeRequest.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeRequest.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateVolumeRequest.MutableParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeRequest.MutableParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:capacity_range, 2, type: Csi.V1.CapacityRange, json_name: "capacityRange")

  field(:volume_capabilities, 3,
    repeated: true,
    type: Csi.V1.VolumeCapability,
    json_name: "volumeCapabilities"
  )

  field(:parameters, 4,
    repeated: true,
    type: Csi.V1.CreateVolumeRequest.ParametersEntry,
    map: true
  )

  field(:secrets, 5,
    repeated: true,
    type: Csi.V1.CreateVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:volume_content_source, 6,
    type: Csi.V1.VolumeContentSource,
    json_name: "volumeContentSource"
  )

  field(:accessibility_requirements, 7,
    type: Csi.V1.TopologyRequirement,
    json_name: "accessibilityRequirements"
  )

  field(:mutable_parameters, 8,
    repeated: true,
    type: Csi.V1.CreateVolumeRequest.MutableParametersEntry,
    json_name: "mutableParameters",
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.VolumeContentSource.SnapshotSource do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeContentSource.SnapshotSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:snapshot_id, 1, type: :string, json_name: "snapshotId")
end

defmodule Csi.V1.VolumeContentSource.VolumeSource do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeContentSource.VolumeSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
end

defmodule Csi.V1.VolumeContentSource do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeContentSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:snapshot, 1, type: Csi.V1.VolumeContentSource.SnapshotSource, oneof: 0)
  field(:volume, 2, type: Csi.V1.VolumeContentSource.VolumeSource, oneof: 0)
end

defmodule Csi.V1.CreateVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume, 1, type: Csi.V1.Volume)
end

defmodule Csi.V1.VolumeCapability.BlockVolume do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeCapability.BlockVolume",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.VolumeCapability.MountVolume do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeCapability.MountVolume",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:fs_type, 1, type: :string, json_name: "fsType")
  field(:mount_flags, 2, repeated: true, type: :string, json_name: "mountFlags")
  field(:volume_mount_group, 3, type: :string, json_name: "volumeMountGroup")
end

defmodule Csi.V1.VolumeCapability.AccessMode do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeCapability.AccessMode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:mode, 1, type: Csi.V1.VolumeCapability.AccessMode.Mode, enum: true)
end

defmodule Csi.V1.VolumeCapability do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeCapability",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:access_type, 0)

  field(:block, 1, type: Csi.V1.VolumeCapability.BlockVolume, oneof: 0)
  field(:mount, 2, type: Csi.V1.VolumeCapability.MountVolume, oneof: 0)
  field(:access_mode, 3, type: Csi.V1.VolumeCapability.AccessMode, json_name: "accessMode")
end

defmodule Csi.V1.CapacityRange do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CapacityRange",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:required_bytes, 1, type: :int64, json_name: "requiredBytes")
  field(:limit_bytes, 2, type: :int64, json_name: "limitBytes")
end

defmodule Csi.V1.Volume.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.Volume.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.Volume do
  @moduledoc false

  use Protobuf, full_name: "csi.v1.Volume", protoc_gen_elixir_version: "0.16.0", syntax: :proto3

  field(:capacity_bytes, 1, type: :int64, json_name: "capacityBytes")
  field(:volume_id, 2, type: :string, json_name: "volumeId")

  field(:volume_context, 3,
    repeated: true,
    type: Csi.V1.Volume.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )

  field(:content_source, 4, type: Csi.V1.VolumeContentSource, json_name: "contentSource")

  field(:accessible_topology, 5,
    repeated: true,
    type: Csi.V1.Topology,
    json_name: "accessibleTopology"
  )
end

defmodule Csi.V1.TopologyRequirement do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.TopologyRequirement",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:requisite, 1, repeated: true, type: Csi.V1.Topology)
  field(:preferred, 2, repeated: true, type: Csi.V1.Topology)
end

defmodule Csi.V1.Topology.SegmentsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.Topology.SegmentsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.Topology do
  @moduledoc false

  use Protobuf, full_name: "csi.v1.Topology", protoc_gen_elixir_version: "0.16.0", syntax: :proto3

  field(:segments, 1, repeated: true, type: Csi.V1.Topology.SegmentsEntry, map: true)
end

defmodule Csi.V1.DeleteVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.DeleteVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")

  field(:secrets, 2,
    repeated: true,
    type: Csi.V1.DeleteVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.DeleteVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.ControllerPublishVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerPublishVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerPublishVolumeRequest.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerPublishVolumeRequest.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerPublishVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerPublishVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:node_id, 2, type: :string, json_name: "nodeId")
  field(:volume_capability, 3, type: Csi.V1.VolumeCapability, json_name: "volumeCapability")
  field(:readonly, 4, type: :bool)

  field(:secrets, 5,
    repeated: true,
    type: Csi.V1.ControllerPublishVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:volume_context, 6,
    repeated: true,
    type: Csi.V1.ControllerPublishVolumeRequest.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )
end

defmodule Csi.V1.ControllerPublishVolumeResponse.PublishContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerPublishVolumeResponse.PublishContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerPublishVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerPublishVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:publish_context, 1,
    repeated: true,
    type: Csi.V1.ControllerPublishVolumeResponse.PublishContextEntry,
    json_name: "publishContext",
    map: true
  )
end

defmodule Csi.V1.ControllerUnpublishVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerUnpublishVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerUnpublishVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerUnpublishVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:node_id, 2, type: :string, json_name: "nodeId")

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.ControllerUnpublishVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.ControllerUnpublishVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerUnpublishVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.ValidateVolumeCapabilitiesRequest.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesRequest.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesRequest.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesRequest.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesRequest.MutableParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesRequest.MutableParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")

  field(:volume_context, 2,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesRequest.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )

  field(:volume_capabilities, 3,
    repeated: true,
    type: Csi.V1.VolumeCapability,
    json_name: "volumeCapabilities"
  )

  field(:parameters, 4,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesRequest.ParametersEntry,
    map: true
  )

  field(:secrets, 5,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:mutable_parameters, 6,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesRequest.MutableParametersEntry,
    json_name: "mutableParameters",
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesResponse.Confirmed.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesResponse.Confirmed.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.MutableParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesResponse.Confirmed.MutableParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesResponse.Confirmed",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_context, 1,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )

  field(:volume_capabilities, 2,
    repeated: true,
    type: Csi.V1.VolumeCapability,
    json_name: "volumeCapabilities"
  )

  field(:parameters, 3,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.ParametersEntry,
    map: true
  )

  field(:mutable_parameters, 4,
    repeated: true,
    type: Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed.MutableParametersEntry,
    json_name: "mutableParameters",
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.ValidateVolumeCapabilitiesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ValidateVolumeCapabilitiesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:confirmed, 1, type: Csi.V1.ValidateVolumeCapabilitiesResponse.Confirmed)
  field(:message, 2, type: :string)
end

defmodule Csi.V1.ListVolumesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListVolumesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:max_entries, 1, type: :int32, json_name: "maxEntries")
  field(:starting_token, 2, type: :string, json_name: "startingToken")
end

defmodule Csi.V1.ListVolumesResponse.VolumeStatus do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListVolumesResponse.VolumeStatus",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:published_node_ids, 1, repeated: true, type: :string, json_name: "publishedNodeIds")

  field(:volume_condition, 2,
    type: Csi.V1.VolumeCondition,
    json_name: "volumeCondition",
    deprecated: false
  )
end

defmodule Csi.V1.ListVolumesResponse.Entry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListVolumesResponse.Entry",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume, 1, type: Csi.V1.Volume)
  field(:status, 2, type: Csi.V1.ListVolumesResponse.VolumeStatus)
end

defmodule Csi.V1.ListVolumesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListVolumesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:entries, 1, repeated: true, type: Csi.V1.ListVolumesResponse.Entry)
  field(:next_token, 2, type: :string, json_name: "nextToken")
end

defmodule Csi.V1.ControllerGetVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerGetVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
end

defmodule Csi.V1.ControllerGetVolumeResponse.VolumeStatus do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerGetVolumeResponse.VolumeStatus",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:published_node_ids, 1, repeated: true, type: :string, json_name: "publishedNodeIds")
  field(:volume_condition, 2, type: Csi.V1.VolumeCondition, json_name: "volumeCondition")
end

defmodule Csi.V1.ControllerGetVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerGetVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume, 1, type: Csi.V1.Volume)
  field(:status, 2, type: Csi.V1.ControllerGetVolumeResponse.VolumeStatus)
end

defmodule Csi.V1.ControllerModifyVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerModifyVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerModifyVolumeRequest.MutableParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerModifyVolumeRequest.MutableParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerModifyVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerModifyVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")

  field(:secrets, 2,
    repeated: true,
    type: Csi.V1.ControllerModifyVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:mutable_parameters, 3,
    repeated: true,
    type: Csi.V1.ControllerModifyVolumeRequest.MutableParametersEntry,
    json_name: "mutableParameters",
    map: true
  )
end

defmodule Csi.V1.ControllerModifyVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerModifyVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.GetCapacityRequest.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetCapacityRequest.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.GetCapacityRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetCapacityRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_capabilities, 1,
    repeated: true,
    type: Csi.V1.VolumeCapability,
    json_name: "volumeCapabilities"
  )

  field(:parameters, 2,
    repeated: true,
    type: Csi.V1.GetCapacityRequest.ParametersEntry,
    map: true
  )

  field(:accessible_topology, 3, type: Csi.V1.Topology, json_name: "accessibleTopology")
end

defmodule Csi.V1.GetCapacityResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetCapacityResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:available_capacity, 1, type: :int64, json_name: "availableCapacity")
  field(:maximum_volume_size, 2, type: Google.Protobuf.Int64Value, json_name: "maximumVolumeSize")

  field(:minimum_volume_size, 3,
    type: Google.Protobuf.Int64Value,
    json_name: "minimumVolumeSize",
    deprecated: false
  )
end

defmodule Csi.V1.ControllerGetCapabilitiesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerGetCapabilitiesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.ControllerGetCapabilitiesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerGetCapabilitiesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capabilities, 1, repeated: true, type: Csi.V1.ControllerServiceCapability)
end

defmodule Csi.V1.ControllerServiceCapability.RPC do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerServiceCapability.RPC",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: Csi.V1.ControllerServiceCapability.RPC.Type, enum: true)
end

defmodule Csi.V1.ControllerServiceCapability do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerServiceCapability",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:rpc, 1, type: Csi.V1.ControllerServiceCapability.RPC, oneof: 0)
end

defmodule Csi.V1.CreateSnapshotRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateSnapshotRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateSnapshotRequest.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateSnapshotRequest.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateSnapshotRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateSnapshotRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:source_volume_id, 1, type: :string, json_name: "sourceVolumeId")
  field(:name, 2, type: :string)

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.CreateSnapshotRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:parameters, 4,
    repeated: true,
    type: Csi.V1.CreateSnapshotRequest.ParametersEntry,
    map: true
  )
end

defmodule Csi.V1.CreateSnapshotResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateSnapshotResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:snapshot, 1, type: Csi.V1.Snapshot)
end

defmodule Csi.V1.Snapshot do
  @moduledoc false

  use Protobuf, full_name: "csi.v1.Snapshot", protoc_gen_elixir_version: "0.16.0", syntax: :proto3

  field(:size_bytes, 1, type: :int64, json_name: "sizeBytes")
  field(:snapshot_id, 2, type: :string, json_name: "snapshotId")
  field(:source_volume_id, 3, type: :string, json_name: "sourceVolumeId")
  field(:creation_time, 4, type: Google.Protobuf.Timestamp, json_name: "creationTime")
  field(:ready_to_use, 5, type: :bool, json_name: "readyToUse")
  field(:group_snapshot_id, 6, type: :string, json_name: "groupSnapshotId", deprecated: false)
end

defmodule Csi.V1.DeleteSnapshotRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteSnapshotRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.DeleteSnapshotRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteSnapshotRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:snapshot_id, 1, type: :string, json_name: "snapshotId")

  field(:secrets, 2,
    repeated: true,
    type: Csi.V1.DeleteSnapshotRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.DeleteSnapshotResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteSnapshotResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.ListSnapshotsRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListSnapshotsRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ListSnapshotsRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListSnapshotsRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:max_entries, 1, type: :int32, json_name: "maxEntries")
  field(:starting_token, 2, type: :string, json_name: "startingToken")
  field(:source_volume_id, 3, type: :string, json_name: "sourceVolumeId")
  field(:snapshot_id, 4, type: :string, json_name: "snapshotId")

  field(:secrets, 5,
    repeated: true,
    type: Csi.V1.ListSnapshotsRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.ListSnapshotsResponse.Entry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListSnapshotsResponse.Entry",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:snapshot, 1, type: Csi.V1.Snapshot)
end

defmodule Csi.V1.ListSnapshotsResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ListSnapshotsResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:entries, 1, repeated: true, type: Csi.V1.ListSnapshotsResponse.Entry)
  field(:next_token, 2, type: :string, json_name: "nextToken")
end

defmodule Csi.V1.ControllerExpandVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerExpandVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.ControllerExpandVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerExpandVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:capacity_range, 2, type: Csi.V1.CapacityRange, json_name: "capacityRange")

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.ControllerExpandVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:volume_capability, 4, type: Csi.V1.VolumeCapability, json_name: "volumeCapability")
end

defmodule Csi.V1.ControllerExpandVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.ControllerExpandVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capacity_bytes, 1, type: :int64, json_name: "capacityBytes")
  field(:node_expansion_required, 2, type: :bool, json_name: "nodeExpansionRequired")
end

defmodule Csi.V1.NodeStageVolumeRequest.PublishContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeStageVolumeRequest.PublishContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodeStageVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeStageVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodeStageVolumeRequest.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeStageVolumeRequest.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodeStageVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeStageVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")

  field(:publish_context, 2,
    repeated: true,
    type: Csi.V1.NodeStageVolumeRequest.PublishContextEntry,
    json_name: "publishContext",
    map: true
  )

  field(:staging_target_path, 3, type: :string, json_name: "stagingTargetPath")
  field(:volume_capability, 4, type: Csi.V1.VolumeCapability, json_name: "volumeCapability")

  field(:secrets, 5,
    repeated: true,
    type: Csi.V1.NodeStageVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:volume_context, 6,
    repeated: true,
    type: Csi.V1.NodeStageVolumeRequest.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )
end

defmodule Csi.V1.NodeStageVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeStageVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodeUnstageVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeUnstageVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:staging_target_path, 2, type: :string, json_name: "stagingTargetPath")
end

defmodule Csi.V1.NodeUnstageVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeUnstageVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodePublishVolumeRequest.PublishContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodePublishVolumeRequest.PublishContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodePublishVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodePublishVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodePublishVolumeRequest.VolumeContextEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodePublishVolumeRequest.VolumeContextEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodePublishVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodePublishVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")

  field(:publish_context, 2,
    repeated: true,
    type: Csi.V1.NodePublishVolumeRequest.PublishContextEntry,
    json_name: "publishContext",
    map: true
  )

  field(:staging_target_path, 3, type: :string, json_name: "stagingTargetPath")
  field(:target_path, 4, type: :string, json_name: "targetPath")
  field(:volume_capability, 5, type: Csi.V1.VolumeCapability, json_name: "volumeCapability")
  field(:readonly, 6, type: :bool)

  field(:secrets, 7,
    repeated: true,
    type: Csi.V1.NodePublishVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:volume_context, 8,
    repeated: true,
    type: Csi.V1.NodePublishVolumeRequest.VolumeContextEntry,
    json_name: "volumeContext",
    map: true
  )
end

defmodule Csi.V1.NodePublishVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodePublishVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodeUnpublishVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeUnpublishVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:target_path, 2, type: :string, json_name: "targetPath")
end

defmodule Csi.V1.NodeUnpublishVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeUnpublishVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodeGetVolumeStatsRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetVolumeStatsRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:volume_path, 2, type: :string, json_name: "volumePath")
  field(:staging_target_path, 3, type: :string, json_name: "stagingTargetPath")
end

defmodule Csi.V1.NodeGetVolumeStatsResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetVolumeStatsResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:usage, 1, repeated: true, type: Csi.V1.VolumeUsage)

  field(:volume_condition, 2,
    type: Csi.V1.VolumeCondition,
    json_name: "volumeCondition",
    deprecated: false
  )
end

defmodule Csi.V1.VolumeUsage do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeUsage",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:available, 1, type: :int64)
  field(:total, 2, type: :int64)
  field(:used, 3, type: :int64)
  field(:unit, 4, type: Csi.V1.VolumeUsage.Unit, enum: true)
end

defmodule Csi.V1.VolumeCondition do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeCondition",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:abnormal, 1, type: :bool)
  field(:message, 2, type: :string)
end

defmodule Csi.V1.NodeGetCapabilitiesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetCapabilitiesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodeGetCapabilitiesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetCapabilitiesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capabilities, 1, repeated: true, type: Csi.V1.NodeServiceCapability)
end

defmodule Csi.V1.NodeServiceCapability.RPC do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeServiceCapability.RPC",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: Csi.V1.NodeServiceCapability.RPC.Type, enum: true)
end

defmodule Csi.V1.NodeServiceCapability do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeServiceCapability",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:rpc, 1, type: Csi.V1.NodeServiceCapability.RPC, oneof: 0)
end

defmodule Csi.V1.NodeGetInfoRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetInfoRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.NodeGetInfoResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeGetInfoResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:node_id, 1, type: :string, json_name: "nodeId")
  field(:max_volumes_per_node, 2, type: :int64, json_name: "maxVolumesPerNode")
  field(:accessible_topology, 3, type: Csi.V1.Topology, json_name: "accessibleTopology")
end

defmodule Csi.V1.NodeExpandVolumeRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeExpandVolumeRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.NodeExpandVolumeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeExpandVolumeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:volume_id, 1, type: :string, json_name: "volumeId")
  field(:volume_path, 2, type: :string, json_name: "volumePath")
  field(:capacity_range, 3, type: Csi.V1.CapacityRange, json_name: "capacityRange")
  field(:staging_target_path, 4, type: :string, json_name: "stagingTargetPath")
  field(:volume_capability, 5, type: Csi.V1.VolumeCapability, json_name: "volumeCapability")

  field(:secrets, 6,
    repeated: true,
    type: Csi.V1.NodeExpandVolumeRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.NodeExpandVolumeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.NodeExpandVolumeResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capacity_bytes, 1, type: :int64, json_name: "capacityBytes")
end

defmodule Csi.V1.GroupControllerGetCapabilitiesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GroupControllerGetCapabilitiesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.GroupControllerGetCapabilitiesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GroupControllerGetCapabilitiesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:capabilities, 1, repeated: true, type: Csi.V1.GroupControllerServiceCapability)
end

defmodule Csi.V1.GroupControllerServiceCapability.RPC do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GroupControllerServiceCapability.RPC",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: Csi.V1.GroupControllerServiceCapability.RPC.Type, enum: true)
end

defmodule Csi.V1.GroupControllerServiceCapability do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GroupControllerServiceCapability",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:rpc, 1, type: Csi.V1.GroupControllerServiceCapability.RPC, oneof: 0)
end

defmodule Csi.V1.CreateVolumeGroupSnapshotRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeGroupSnapshotRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateVolumeGroupSnapshotRequest.ParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeGroupSnapshotRequest.ParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.CreateVolumeGroupSnapshotRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeGroupSnapshotRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:source_volume_ids, 2, repeated: true, type: :string, json_name: "sourceVolumeIds")

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.CreateVolumeGroupSnapshotRequest.SecretsEntry,
    map: true,
    deprecated: false
  )

  field(:parameters, 4,
    repeated: true,
    type: Csi.V1.CreateVolumeGroupSnapshotRequest.ParametersEntry,
    map: true
  )
end

defmodule Csi.V1.CreateVolumeGroupSnapshotResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.CreateVolumeGroupSnapshotResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:group_snapshot, 1, type: Csi.V1.VolumeGroupSnapshot, json_name: "groupSnapshot")
end

defmodule Csi.V1.VolumeGroupSnapshot do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.VolumeGroupSnapshot",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:group_snapshot_id, 1, type: :string, json_name: "groupSnapshotId")
  field(:snapshots, 2, repeated: true, type: Csi.V1.Snapshot)
  field(:creation_time, 3, type: Google.Protobuf.Timestamp, json_name: "creationTime")
  field(:ready_to_use, 4, type: :bool, json_name: "readyToUse")
end

defmodule Csi.V1.DeleteVolumeGroupSnapshotRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeGroupSnapshotRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.DeleteVolumeGroupSnapshotRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeGroupSnapshotRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:group_snapshot_id, 1, type: :string, json_name: "groupSnapshotId")
  field(:snapshot_ids, 2, repeated: true, type: :string, json_name: "snapshotIds")

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.DeleteVolumeGroupSnapshotRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.DeleteVolumeGroupSnapshotResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.DeleteVolumeGroupSnapshotResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Csi.V1.GetVolumeGroupSnapshotRequest.SecretsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetVolumeGroupSnapshotRequest.SecretsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Csi.V1.GetVolumeGroupSnapshotRequest do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetVolumeGroupSnapshotRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:group_snapshot_id, 1, type: :string, json_name: "groupSnapshotId")
  field(:snapshot_ids, 2, repeated: true, type: :string, json_name: "snapshotIds")

  field(:secrets, 3,
    repeated: true,
    type: Csi.V1.GetVolumeGroupSnapshotRequest.SecretsEntry,
    map: true,
    deprecated: false
  )
end

defmodule Csi.V1.GetVolumeGroupSnapshotResponse do
  @moduledoc false

  use Protobuf,
    full_name: "csi.v1.GetVolumeGroupSnapshotResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:group_snapshot, 1, type: Csi.V1.VolumeGroupSnapshot, json_name: "groupSnapshot")
end

defmodule Csi.V1.Identity.Service do
  @moduledoc false

  use GRPC.Service, name: "csi.v1.Identity", protoc_gen_elixir_version: "0.16.0"

  rpc(:GetPluginInfo, Csi.V1.GetPluginInfoRequest, Csi.V1.GetPluginInfoResponse)

  rpc(
    :GetPluginCapabilities,
    Csi.V1.GetPluginCapabilitiesRequest,
    Csi.V1.GetPluginCapabilitiesResponse
  )

  rpc(:Probe, Csi.V1.ProbeRequest, Csi.V1.ProbeResponse)
end

defmodule Csi.V1.Identity.Stub do
  @moduledoc false

  use GRPC.Stub, service: Csi.V1.Identity.Service
end

defmodule Csi.V1.Controller.Service do
  @moduledoc false

  use GRPC.Service, name: "csi.v1.Controller", protoc_gen_elixir_version: "0.16.0"

  rpc(:CreateVolume, Csi.V1.CreateVolumeRequest, Csi.V1.CreateVolumeResponse)

  rpc(:DeleteVolume, Csi.V1.DeleteVolumeRequest, Csi.V1.DeleteVolumeResponse)

  rpc(
    :ControllerPublishVolume,
    Csi.V1.ControllerPublishVolumeRequest,
    Csi.V1.ControllerPublishVolumeResponse
  )

  rpc(
    :ControllerUnpublishVolume,
    Csi.V1.ControllerUnpublishVolumeRequest,
    Csi.V1.ControllerUnpublishVolumeResponse
  )

  rpc(
    :ValidateVolumeCapabilities,
    Csi.V1.ValidateVolumeCapabilitiesRequest,
    Csi.V1.ValidateVolumeCapabilitiesResponse
  )

  rpc(:ListVolumes, Csi.V1.ListVolumesRequest, Csi.V1.ListVolumesResponse)

  rpc(:GetCapacity, Csi.V1.GetCapacityRequest, Csi.V1.GetCapacityResponse)

  rpc(
    :ControllerGetCapabilities,
    Csi.V1.ControllerGetCapabilitiesRequest,
    Csi.V1.ControllerGetCapabilitiesResponse
  )

  rpc(:CreateSnapshot, Csi.V1.CreateSnapshotRequest, Csi.V1.CreateSnapshotResponse)

  rpc(:DeleteSnapshot, Csi.V1.DeleteSnapshotRequest, Csi.V1.DeleteSnapshotResponse)

  rpc(:ListSnapshots, Csi.V1.ListSnapshotsRequest, Csi.V1.ListSnapshotsResponse)

  rpc(
    :ControllerExpandVolume,
    Csi.V1.ControllerExpandVolumeRequest,
    Csi.V1.ControllerExpandVolumeResponse
  )

  rpc(:ControllerGetVolume, Csi.V1.ControllerGetVolumeRequest, Csi.V1.ControllerGetVolumeResponse)

  rpc(
    :ControllerModifyVolume,
    Csi.V1.ControllerModifyVolumeRequest,
    Csi.V1.ControllerModifyVolumeResponse
  )
end

defmodule Csi.V1.Controller.Stub do
  @moduledoc false

  use GRPC.Stub, service: Csi.V1.Controller.Service
end

defmodule Csi.V1.GroupController.Service do
  @moduledoc false

  use GRPC.Service, name: "csi.v1.GroupController", protoc_gen_elixir_version: "0.16.0"

  rpc(
    :GroupControllerGetCapabilities,
    Csi.V1.GroupControllerGetCapabilitiesRequest,
    Csi.V1.GroupControllerGetCapabilitiesResponse
  )

  rpc(
    :CreateVolumeGroupSnapshot,
    Csi.V1.CreateVolumeGroupSnapshotRequest,
    Csi.V1.CreateVolumeGroupSnapshotResponse
  )

  rpc(
    :DeleteVolumeGroupSnapshot,
    Csi.V1.DeleteVolumeGroupSnapshotRequest,
    Csi.V1.DeleteVolumeGroupSnapshotResponse
  )

  rpc(
    :GetVolumeGroupSnapshot,
    Csi.V1.GetVolumeGroupSnapshotRequest,
    Csi.V1.GetVolumeGroupSnapshotResponse
  )
end

defmodule Csi.V1.GroupController.Stub do
  @moduledoc false

  use GRPC.Stub, service: Csi.V1.GroupController.Service
end

defmodule Csi.V1.Node.Service do
  @moduledoc false

  use GRPC.Service, name: "csi.v1.Node", protoc_gen_elixir_version: "0.16.0"

  rpc(:NodeStageVolume, Csi.V1.NodeStageVolumeRequest, Csi.V1.NodeStageVolumeResponse)

  rpc(:NodeUnstageVolume, Csi.V1.NodeUnstageVolumeRequest, Csi.V1.NodeUnstageVolumeResponse)

  rpc(:NodePublishVolume, Csi.V1.NodePublishVolumeRequest, Csi.V1.NodePublishVolumeResponse)

  rpc(:NodeUnpublishVolume, Csi.V1.NodeUnpublishVolumeRequest, Csi.V1.NodeUnpublishVolumeResponse)

  rpc(:NodeGetVolumeStats, Csi.V1.NodeGetVolumeStatsRequest, Csi.V1.NodeGetVolumeStatsResponse)

  rpc(:NodeExpandVolume, Csi.V1.NodeExpandVolumeRequest, Csi.V1.NodeExpandVolumeResponse)

  rpc(:NodeGetCapabilities, Csi.V1.NodeGetCapabilitiesRequest, Csi.V1.NodeGetCapabilitiesResponse)

  rpc(:NodeGetInfo, Csi.V1.NodeGetInfoRequest, Csi.V1.NodeGetInfoResponse)
end

defmodule Csi.V1.Node.Stub do
  @moduledoc false

  use GRPC.Stub, service: Csi.V1.Node.Service
end
