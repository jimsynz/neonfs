# NIF for NeonFS.Core.Blob.Native

## To build the NIF module:

- Your NIF will now build along with your project.

## To load the NIF:

```elixir
defmodule NeonFS.Core.Blob.Native do
  use Rustler, otp_app: :neonfs_core, crate: "neonfs_blob"

  # When your NIF is loaded, it will override this function.
  def add(_a, _b), do: :erlang.nif_error(:nif_not_loaded)
end
```

## Examples

[This](https://github.com/rusterlium/NifIo) is a complete example of a NIF written in Rust.
