defmodule Csi.V1.PbExtension do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.16.0"

  extend(Google.Protobuf.EnumOptions, :alpha_enum, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaEnum"
  )

  extend(Google.Protobuf.EnumValueOptions, :alpha_enum_value, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaEnumValue"
  )

  extend(Google.Protobuf.FieldOptions, :csi_secret, 1059,
    optional: true,
    type: :bool,
    json_name: "csiSecret"
  )

  extend(Google.Protobuf.FieldOptions, :alpha_field, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaField"
  )

  extend(Google.Protobuf.MessageOptions, :alpha_message, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaMessage"
  )

  extend(Google.Protobuf.MethodOptions, :alpha_method, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaMethod"
  )

  extend(Google.Protobuf.ServiceOptions, :alpha_service, 1060,
    optional: true,
    type: :bool,
    json_name: "alphaService"
  )
end
