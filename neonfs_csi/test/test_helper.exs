Mimic.copy(NeonFS.Client.Discovery)

# The csi-sanity conformance test shells out to the upstream `csi-sanity`
# binary. Exclude it unless that binary is available (on PATH or via the
# CSI_SANITY env var) so the suite skips gracefully where the tool is
# absent and runs where CI installs it.
csi_sanity? =
  System.get_env("CSI_SANITY") not in [nil, ""] or
    System.find_executable("csi-sanity") != nil

exclude = if csi_sanity?, do: [], else: [:requires_csi_sanity]

ExUnit.start(capture_log: true, exclude: exclude)
