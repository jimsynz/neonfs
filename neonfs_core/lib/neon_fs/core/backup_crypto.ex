defmodule NeonFS.Core.BackupCrypto do
  @moduledoc """
  At-rest encryption for backup archives (#1004, part of #248).

  A backup is encrypted under a fresh per-archive **content key** (32
  random bytes). File bodies are encrypted frame-at-a-time with
  AES-256-GCM so the export/import streaming model is preserved — the
  working set is one frame, never the whole file. The content key is
  itself wrapped under a key-encryption key (KEK) derived from the
  operator's passphrase via PBKDF2-HMAC-SHA256; the wrapped key plus
  KDF parameters travel in the archive manifest's plaintext
  `encryption` envelope.

  Only file bodies are encrypted — the manifest (and therefore the
  path list) stays plaintext, which is what keeps incremental diffs
  and `Backup.describe/1` working against an encrypted archive. Path
  confidentiality is out of scope for this slice.

  ## Frame layout

  Each frame on disk is `nonce(12) ‖ ciphertext ‖ tag(16)`. The
  ciphertext is the same length as the plaintext (GCM is a stream
  cipher), so frame overhead is a fixed 28 bytes. Every frame but the
  last carries `frame_size/0` plaintext bytes; the last carries the
  remainder. The frame index and the file's full path are bound as
  AAD, so frames cannot be reordered within a file nor swapped between
  files that share the per-archive key.

  All primitives are OTP `:crypto` — no third-party dependency.
  """

  @envelope_version 1
  @cipher "AES-256-GCM"
  @kdf "PBKDF2-HMAC-SHA256"
  @key_source "passphrase"

  @frame_size 1024 * 1024
  @nonce_size 12
  @tag_size 16
  @key_size 32
  @salt_size 32
  @default_iters 600_000
  @frame_overhead @nonce_size + @tag_size

  # Domain-separation AAD for the content-key wrap, so a wrapped key
  # can never be reinterpreted as a file frame.
  @keywrap_aad "neonfs.backup.keywrap.v1"

  @type envelope :: %{String.t() => term()}

  @doc "Plaintext bytes per full frame."
  @spec frame_size() :: pos_integer()
  def frame_size, do: @frame_size

  @doc "Fixed per-frame overhead (nonce + tag) in bytes."
  @spec frame_overhead() :: pos_integer()
  def frame_overhead, do: @frame_overhead

  @doc """
  Generate a fresh per-archive content key and the plaintext envelope
  that wraps it under `passphrase`. The envelope is embedded verbatim
  in the manifest; `open/2` reverses it.
  """
  @spec seal(binary()) :: {:ok, binary(), envelope()}
  def seal(passphrase) when is_binary(passphrase) do
    content_key = :crypto.strong_rand_bytes(@key_size)
    salt = :crypto.strong_rand_bytes(@salt_size)
    kek = derive_kek(passphrase, salt, @default_iters)
    wrap_nonce = :crypto.strong_rand_bytes(@nonce_size)

    {ct, tag} =
      :crypto.crypto_one_time_aead(
        :aes_256_gcm,
        kek,
        wrap_nonce,
        content_key,
        @keywrap_aad,
        true
      )

    envelope = %{
      "version" => @envelope_version,
      "cipher" => @cipher,
      "kdf" => @kdf,
      "kdf_salt" => Base.encode64(salt),
      "kdf_iters" => @default_iters,
      "frame_size" => @frame_size,
      "key_source" => @key_source,
      "wrapped_key" => Base.encode64(wrap_nonce <> ct <> tag)
    }

    {:ok, content_key, envelope}
  end

  @doc """
  Recover the content key from `envelope` using `passphrase`. Returns
  `{:error, :bad_passphrase}` on a wrap-tag mismatch (wrong passphrase
  or tampered envelope) and `{:error, :malformed_envelope}` if the
  envelope is structurally invalid. A wrong passphrase is caught here,
  before any file body is touched.
  """
  @spec open(envelope(), binary()) ::
          {:ok, binary()} | {:error, :bad_passphrase | :malformed_envelope}
  def open(envelope, passphrase) when is_map(envelope) and is_binary(passphrase) do
    with {:ok, salt} <- decode_b64(envelope["kdf_salt"]),
         {:ok, wrapped} <- decode_b64(envelope["wrapped_key"]),
         iters when is_integer(iters) and iters > 0 <- envelope["kdf_iters"],
         <<wrap_nonce::binary-size(@nonce_size), rest::binary>> <- wrapped,
         ct_size = byte_size(rest) - @tag_size,
         true <- ct_size >= 0,
         <<ct::binary-size(^ct_size), tag::binary-size(@tag_size)>> <- rest do
      kek = derive_kek(passphrase, salt, iters)

      case :crypto.crypto_one_time_aead(
             :aes_256_gcm,
             kek,
             wrap_nonce,
             ct,
             @keywrap_aad,
             tag,
             false
           ) do
        :error -> {:error, :bad_passphrase}
        key when is_binary(key) -> {:ok, key}
      end
    else
      _ -> {:error, :malformed_envelope}
    end
  end

  @doc "Ciphertext body size for a plaintext file of `size` bytes."
  @spec encrypted_size(non_neg_integer()) :: non_neg_integer()
  def encrypted_size(0), do: 0
  def encrypted_size(size) when size > 0, do: size + frame_count(size) * @frame_overhead

  @doc "Number of frames a plaintext file of `size` bytes splits into."
  @spec frame_count(non_neg_integer()) :: non_neg_integer()
  def frame_count(0), do: 0
  def frame_count(size) when size > 0, do: div(size + @frame_size - 1, @frame_size)

  @doc """
  Plaintext length of frame `index` for a file of `size` bytes split
  into `n_frames`. Every frame but the last is `frame_size/0`; the
  last carries the remainder.
  """
  @spec frame_plaintext_len(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          non_neg_integer()
  def frame_plaintext_len(size, index, n_frames) when index == n_frames - 1 do
    size - (n_frames - 1) * @frame_size
  end

  def frame_plaintext_len(_size, _index, _n_frames), do: @frame_size

  @doc "Encrypt one plaintext frame into `nonce ‖ ciphertext ‖ tag`."
  @spec encrypt_frame(binary(), binary(), binary(), non_neg_integer()) :: binary()
  def encrypt_frame(plaintext, content_key, path, index) when is_binary(plaintext) do
    nonce = :crypto.strong_rand_bytes(@nonce_size)

    {ct, tag} =
      :crypto.crypto_one_time_aead(
        :aes_256_gcm,
        content_key,
        nonce,
        plaintext,
        frame_aad(path, index),
        true
      )

    nonce <> ct <> tag
  end

  @doc """
  Decrypt one `nonce ‖ ciphertext ‖ tag` frame. `{:error,
  :corrupt_frame}` on a tag mismatch (wrong key, reordering, or
  corruption).
  """
  @spec decrypt_frame(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, binary()} | {:error, :corrupt_frame}
  def decrypt_frame(<<nonce::binary-size(@nonce_size), rest::binary>>, content_key, path, index)
      when byte_size(rest) >= @tag_size do
    ct_size = byte_size(rest) - @tag_size
    <<ct::binary-size(^ct_size), tag::binary-size(@tag_size)>> = rest

    case :crypto.crypto_one_time_aead(
           :aes_256_gcm,
           content_key,
           nonce,
           ct,
           frame_aad(path, index),
           tag,
           false
         ) do
      :error -> {:error, :corrupt_frame}
      plaintext when is_binary(plaintext) -> {:ok, plaintext}
    end
  end

  def decrypt_frame(_frame, _content_key, _path, _index), do: {:error, :corrupt_frame}

  ## Private

  defp frame_aad(path, index), do: <<index::unsigned-big-64, path::binary>>

  defp derive_kek(passphrase, salt, iters) do
    :crypto.pbkdf2_hmac(:sha256, passphrase, salt, iters, @key_size)
  end

  defp decode_b64(value) when is_binary(value), do: Base.decode64(value)
  defp decode_b64(_), do: :error
end
