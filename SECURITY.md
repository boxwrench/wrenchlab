# Security

WrenchLab is an early research tool, not a hostile-code sandbox or a
multi-tenant service. Do not run untrusted commands with a worker account.

Never commit credentials, private keys, raw environment files, authorization
headers, private host configuration or live evidence. Use the placeholders in
`.env.example` and `config/examples/`. Keep local configuration under ignored
paths. Report suspected vulnerabilities privately to the repository owner
before public disclosure.
