[
  # x509 dependency uses ASN.1 record types and OTP types that dialyzer can't resolve
  {"lib/x509/certificate.ex", :unknown_type},
  {"lib/x509/crl/entry.ex", :unknown_type},
  {"lib/x509/crl/extension.ex", :unknown_type},
  {"lib/x509/csr.ex", :unknown_type},
  {"lib/x509/private_key.ex", :unknown_type}
]
