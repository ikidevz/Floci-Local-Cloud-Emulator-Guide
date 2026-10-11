"""python demo.py -> seed the two secrets, rotate one, read an old version, resolve app settings."""
import kv

c = kv.client()
c.set_secret("pii-salt", "dev-only-salt")
v1 = c.set_secret("sql-password", "Str0ng!Passw0rd")
v2 = c.set_secret("sql-password", "N3w!Passw0rd-rotated")                 # rotation = a NEW immutable version
assert c.get_secret("sql-password").value == "N3w!Passw0rd-rotated"        # 'latest' follows the newest version
assert c.get_secret("sql-password", v1.properties.version).value == "Str0ng!Passw0rd"   # the old one is still addressable
print("versions:", [p.version[:8] for p in c.list_properties_of_secret_versions("sql-password")])

print("app settings the Function would receive:", kv.app_settings_from_vault(c, {"PII_SALT": "pii-salt", "SQL_PASSWORD": "sql-password"}))
print("on real Azure that setting is instead:   ", kv.kv_reference("kv-demo-prod", "sql-password"))
