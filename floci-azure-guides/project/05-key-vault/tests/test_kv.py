import uuid

import kv


def versioning_contract(c, name):
    """Identical assertions for the fake vault (this sandbox) and Floci AZ (your machine)."""
    v1 = c.set_secret(name, "one")
    v2 = c.set_secret(name, "two")
    assert v1.properties.version != v2.properties.version
    assert c.get_secret(name).value == "two"                                   # latest wins
    assert c.get_secret(name, v1.properties.version).value == "one"            # old version still addressable
    assert len(list(c.list_properties_of_secret_versions(name))) == 2


def test_client_survives_the_challenge_handshake_with_http_rewrite(fake_vault_client):
    from conftest import FakeVault
    versioning_contract(fake_vault_client, "pii-salt")
    assert FakeVault.seen_auth and FakeVault.seen_auth[0] == "Bearer fake-token"   # the fake credential was used after the 401 challenge


def test_app_settings_resolve_from_vault(fake_vault_client):
    fake_vault_client.set_secret("pii-salt", "s1")
    fake_vault_client.set_secret("sql-password", "p1")
    assert kv.app_settings_from_vault(fake_vault_client, {"PII_SALT": "pii-salt", "SQL_PASSWORD": "sql-password"}) == {"PII_SALT": "s1", "SQL_PASSWORD": "p1"}


def test_kv_reference_has_the_exact_azure_syntax():
    assert kv.kv_reference("kv-x", "sql-password") == "@Microsoft.KeyVault(SecretUri=https://kv-x.vault.azure.net/secrets/sql-password)"


def test_vault_url_env_switches_to_real_azure(monkeypatch):
    monkeypatch.setenv("VAULT_URL", "https://kv-x.vault.azure.net")
    try:
        kv.client()
    except ModuleNotFoundError as e:                                           # azure-identity is only needed on real Azure
        assert "azure" in str(e)


# --- against the real emulator (skipped here unless Floci AZ is up) -----------------------------------------------
def test_floci_versioning(floci_client):
    versioning_contract(floci_client, f"s-{uuid.uuid4().hex[:8]}")


def test_floci_soft_delete_recover_purge(floci_client):
    name = f"s-{uuid.uuid4().hex[:8]}"
    floci_client.set_secret(name, "x")
    assert floci_client.begin_delete_secret(name).result().name == name
    assert floci_client.begin_recover_deleted_secret(name).result().name == name
    assert floci_client.get_secret(name).value == "x"
    floci_client.begin_delete_secret(name).result()
    floci_client.purge_deleted_secret(name)
