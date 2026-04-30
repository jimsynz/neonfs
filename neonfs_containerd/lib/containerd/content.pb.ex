defmodule Containerd.Services.Content.V1.WriteAction do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:STAT, 0)
  field(:WRITE, 1)
  field(:COMMIT, 2)
end

defmodule Containerd.Services.Content.V1.Info.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Containerd.Services.Content.V1.Info do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:digest, 1, type: :string)
  field(:size, 2, type: :int64)
  field(:created_at, 3, type: Google.Protobuf.Timestamp, json_name: "createdAt")
  field(:updated_at, 4, type: Google.Protobuf.Timestamp, json_name: "updatedAt")

  field(:labels, 5,
    repeated: true,
    type: Containerd.Services.Content.V1.Info.LabelsEntry,
    map: true
  )
end

defmodule Containerd.Services.Content.V1.InfoRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:digest, 1, type: :string)
end

defmodule Containerd.Services.Content.V1.InfoResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:info, 1, type: Containerd.Services.Content.V1.Info)
end

defmodule Containerd.Services.Content.V1.UpdateRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:info, 1, type: Containerd.Services.Content.V1.Info)
  field(:update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask")
end

defmodule Containerd.Services.Content.V1.UpdateResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:info, 1, type: Containerd.Services.Content.V1.Info)
end

defmodule Containerd.Services.Content.V1.ListContentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:filters, 1, repeated: true, type: :string)
end

defmodule Containerd.Services.Content.V1.ListContentResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:info, 1, repeated: true, type: Containerd.Services.Content.V1.Info)
end

defmodule Containerd.Services.Content.V1.DeleteContentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:digest, 1, type: :string)
end

defmodule Containerd.Services.Content.V1.ReadContentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:digest, 1, type: :string)
  field(:offset, 2, type: :int64)
  field(:size, 3, type: :int64)
end

defmodule Containerd.Services.Content.V1.ReadContentResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:offset, 1, type: :int64)
  field(:data, 2, type: :bytes)
end

defmodule Containerd.Services.Content.V1.Status do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:started_at, 1, type: Google.Protobuf.Timestamp, json_name: "startedAt")
  field(:updated_at, 2, type: Google.Protobuf.Timestamp, json_name: "updatedAt")
  field(:ref, 3, type: :string)
  field(:offset, 4, type: :int64)
  field(:total, 5, type: :int64)
  field(:expected, 6, type: :string)
end

defmodule Containerd.Services.Content.V1.StatusRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:ref, 1, type: :string)
end

defmodule Containerd.Services.Content.V1.StatusResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:status, 1, type: Containerd.Services.Content.V1.Status)
end

defmodule Containerd.Services.Content.V1.ListStatusesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:filters, 1, repeated: true, type: :string)
end

defmodule Containerd.Services.Content.V1.ListStatusesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:statuses, 1, repeated: true, type: Containerd.Services.Content.V1.Status)
end

defmodule Containerd.Services.Content.V1.WriteContentRequest.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Containerd.Services.Content.V1.WriteContentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:action, 1, type: Containerd.Services.Content.V1.WriteAction, enum: true)
  field(:ref, 2, type: :string)
  field(:total, 3, type: :int64)
  field(:expected, 4, type: :string)
  field(:offset, 5, type: :int64)
  field(:data, 6, type: :bytes)

  field(:labels, 7,
    repeated: true,
    type: Containerd.Services.Content.V1.WriteContentRequest.LabelsEntry,
    map: true
  )
end

defmodule Containerd.Services.Content.V1.WriteContentResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:action, 1, type: Containerd.Services.Content.V1.WriteAction, enum: true)
  field(:started_at, 2, type: Google.Protobuf.Timestamp, json_name: "startedAt")
  field(:updated_at, 3, type: Google.Protobuf.Timestamp, json_name: "updatedAt")
  field(:offset, 4, type: :int64)
  field(:total, 5, type: :int64)
  field(:digest, 6, type: :string)
end

defmodule Containerd.Services.Content.V1.AbortRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:ref, 1, type: :string)
end

defmodule Containerd.Services.Content.V1.Content.Service do
  @moduledoc false

  use GRPC.Service,
    name: "containerd.services.content.v1.Content",
    protoc_gen_elixir_version: "0.12.0"

  rpc(
    :Info,
    Containerd.Services.Content.V1.InfoRequest,
    Containerd.Services.Content.V1.InfoResponse
  )

  rpc(
    :Update,
    Containerd.Services.Content.V1.UpdateRequest,
    Containerd.Services.Content.V1.UpdateResponse
  )

  rpc(
    :List,
    Containerd.Services.Content.V1.ListContentRequest,
    stream(Containerd.Services.Content.V1.ListContentResponse)
  )

  rpc(:Delete, Containerd.Services.Content.V1.DeleteContentRequest, Google.Protobuf.Empty)

  rpc(
    :Read,
    Containerd.Services.Content.V1.ReadContentRequest,
    stream(Containerd.Services.Content.V1.ReadContentResponse)
  )

  rpc(
    :Status,
    Containerd.Services.Content.V1.StatusRequest,
    Containerd.Services.Content.V1.StatusResponse
  )

  rpc(
    :ListStatuses,
    Containerd.Services.Content.V1.ListStatusesRequest,
    Containerd.Services.Content.V1.ListStatusesResponse
  )

  rpc(
    :Write,
    stream(Containerd.Services.Content.V1.WriteContentRequest),
    stream(Containerd.Services.Content.V1.WriteContentResponse)
  )

  rpc(:Abort, Containerd.Services.Content.V1.AbortRequest, Google.Protobuf.Empty)
end

defmodule Containerd.Services.Content.V1.Content.Stub do
  @moduledoc false

  use GRPC.Stub, service: Containerd.Services.Content.V1.Content.Service
end
