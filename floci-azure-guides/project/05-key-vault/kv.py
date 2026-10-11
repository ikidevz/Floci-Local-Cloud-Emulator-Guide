"""Key Vault for the pipelines' two secrets: the PII tokenisation salt and the SQL password.

Locally the Python SDK needs three workarounds (Floci AZ docs): the SDK ENFORCES https and a challenge-based auth flow, so we
  1. give it a fake credential,   2. rewrite https->http in a transport,   3. turn off challenge-resource verification.
On real Azure set VAULT_URL=https://<vault>.vault.azure.net and the same code uses the Function's managed identity.
"""
import os
import re
import time

from azure.core.credentials import AccessToken, TokenCredential
from azure.core.pipeline.transport import RequestsTransport
from azure.keyvault.secrets import SecretClient

ACCOUNT = "devstoreaccount1"


class FakeCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        return AccessToken("fake-token", int(time.time()) + 3600)


class ForceHttpTransport(RequestsTransport):
    """Rewrite https->http for the wire only. The URL is RESTORED afterwards: the SDK's auth policy re-checks for https on every
    attempt, so a permanent rewrite breaks the retry that follows a 401 challenge (Floci's own snippet only works if no challenge is issued)."""
    def send(self, request, **kwargs):
        original = request.url
        request.url = original.replace("https://", "http://", 1)
        try:
            return super().send(request, **kwargs)
        finally:
            request.url = original


def client(endpoint=None, **kw):
    vault_url = os.environ.get("VAULT_URL")
    if vault_url:                                                      # real Azure: no secret anywhere, the identity is the credential
        from azure.identity import DefaultAzureCredential
        return SecretClient(vault_url, DefaultAzureCredential(), **kw)
    base = endpoint or os.environ.get("FLOCI_ENDPOINT", "http://localhost:4577")
    return SecretClient(vault_url=re.sub(r"^http://", "https://", base) + f"/{ACCOUNT}-keyvault", credential=FakeCredential(),
                        transport=ForceHttpTransport(), verify_challenge_resource=False, **kw)


def app_settings_from_vault(c, mapping):
    """{ENV_VAR: secret-name} -> {ENV_VAR: value}. This is what a Key Vault REFERENCE does for you on real Azure; Floci AZ does not
    resolve references, so locally the deploy script injects the resolved values as the function app's environment."""
    return {env: c.get_secret(secret).value for env, secret in mapping.items()}


def kv_reference(vault_name, secret):
    """The app-setting VALUE real Azure resolves at runtime (what the Terraform in the pipelines writes). Nothing is stored in the app."""
    return f"@Microsoft.KeyVault(SecretUri=https://{vault_name}.vault.azure.net/secrets/{secret})"
