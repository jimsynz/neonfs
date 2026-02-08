# NeonFS Client

Shared types and service discovery client for NeonFS cluster members.

This is a pure library package (no OTP application). Consumers start its
GenServers in their own supervision trees.

## Usage

Add to your `mix.exs`:

```elixir
{:neonfs_client, path: "../neonfs_client"}
```
