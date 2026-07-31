defmodule NeonFS.FUSE.LoggerConfigTest do
  use ExUnit.Case, async: true

  # `Logger.Formatter.format/2` only matches a `%Logger.Formatter{}` struct, and
  # `Logger.App` passes the `:default_handler` `:formatter` value through to
  # `:logger` untouched. A keyword list there compiles, boots, and then raises on
  # every format attempt — surfacing as `FORMATTER CRASH` with all metadata
  # dropped, only on logs emitted outside `capture_log`.
  test "the default handler's formatter is built with Logger.Formatter.new/1" do
    formatter = Application.fetch_env!(:logger, :default_handler)[:formatter]

    assert {Logger.Formatter, %Logger.Formatter{}} = formatter
  end

  test "the configured formatter renders a message with its metadata" do
    {mod, config} = Application.fetch_env!(:logger, :default_handler)[:formatter]

    event = %{
      level: :warning,
      msg: {:string, "insufficient drives"},
      meta: %{time: 0, volume_id: "vol-1"}
    }

    rendered = IO.iodata_to_binary(mod.format(event, config))

    assert rendered =~ "insufficient drives"
    assert rendered =~ "volume_id=vol-1"
  end
end
