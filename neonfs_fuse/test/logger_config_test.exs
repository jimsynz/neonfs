defmodule NeonFS.FUSE.LoggerConfigTest do
  use ExUnit.Case, async: true

  # Setting both `:default_formatter` and `:default_handler` is how this
  # package has gone wrong twice. The handler's formatter wins, so the
  # `:default_formatter` list — the one Credo's MissedMetadataKeyInLoggerConfig
  # check reads — becomes decorative, and a key present there but missing from
  # the handler's copy is dropped at render while Credo reports it configured.
  # It is also where the keyword-list formatter shape kept reappearing:
  # `Logger.App` normalises `:default_formatter` into a `%Logger.Formatter{}`
  # but passes a `:default_handler` `:formatter` through to `:logger` untouched,
  # so a keyword list there boots fine and then raises on every format attempt.
  #
  # One source of truth removes both failures at once.
  test "the formatter has one source of truth" do
    refute Application.get_env(:logger, :default_handler)[:formatter],
           "`:default_formatter` alone configures the default handler — setting " <>
             "`:default_handler` as well silently overrides it and reintroduces the drift"

    assert Application.fetch_env!(:logger, :default_formatter)[:metadata]
  end

  test "the declared metadata keys survive a render" do
    {mod, config} = Logger.Formatter.new(Application.fetch_env!(:logger, :default_formatter))

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
