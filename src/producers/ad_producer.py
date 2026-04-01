"""
╔══════════════════════════════════════════════════════════════════════╗
║  Sumerge IT Ops — Active Directory LDAP Producer                    ║
║  Auth: Kerberos GSSAPI via ldap3 (pure Python — works on Windows)   ║
║  Domain: SUMERGEDC.LOCAL  |  DC: ADDC01.sumergedc.local             ║
╚══════════════════════════════════════════════════════════════════════╝

SETUP (Windows — one time only):
    pip install ldap3 confluent-kafka python-dotenv gssapi

AUTHENTICATION:
    No kinit needed on Windows. You are already authenticated via
    your domain login (khatem@sumergedc.local). ldap3 uses your
    existing Windows Kerberos TGT automatically.

    If running as a non-domain account or scheduled task, use:
        AD_USER=khatem@sumergedc.local
        AD_PASSWORD=your_password
    and the script falls back to NTLM.

ADD TO .env:
    LDAP_URI=ldap://ADDC01.sumergedc.local:389
    BASE_DN=DC=sumergedc,DC=local

RUN:
    python producers/ad_producer.py --mode all
    python producers/ad_producer.py --lookup khatem
    python producers/ad_producer.py --group "IT Department"
    python producers/ad_producer.py --mode ous --no-kafka
"""

import os
import sys
import json
import struct
import logging
import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional

from ldap3 import (
    Server, Connection, ALL, SASL, KERBEROS, NTLM,
    SUBTREE, ALL_ATTRIBUTES, SIMPLE,
    extend
)
from ldap3.core.exceptions import (
    LDAPBindError, LDAPSocketOpenError,
    LDAPOperationsErrorResult, LDAPInsufficientAccessRightsResult,
)
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ad_producer")

# ── Config ──────────────────────────────────────────────────────
LDAP_URI     = os.getenv("LDAP_URI",    "ldap://ADDC01.sumergedc.local:389")
BASE_DN      = os.getenv("BASE_DN",     "DC=sumergedc,DC=local")
AD_USER      = os.getenv("AD_USER",     "")   # optional: fallback to NTLM
AD_PASSWORD  = os.getenv("AD_PASSWORD", "")   # optional: fallback to NTLM
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9092")
PAGE_SIZE    = 500

TOPIC_USERS     = "ad.users.raw"
TOPIC_OUS       = "ad.ous.raw"
TOPIC_GROUPS    = "ad.groups.raw"
TOPIC_COMPUTERS = "ad.computers.raw"

UAC_FLAGS = {
    0x0002:   "ACCOUNTDISABLE",
    0x0010:   "LOCKOUT",
    0x0020:   "PASSWD_NOTREQD",
    0x0040:   "PASSWD_CANT_CHANGE",
    0x10000:  "DONT_EXPIRE_PASSWD",
    0x800000: "TRUSTED_FOR_DELEGATION",
}


# ── Helpers ─────────────────────────────────────────────────────
def decode_sid(sid_bytes: bytes) -> str:
    if not sid_bytes:
        return ""
    try:
        revision       = sid_bytes[0]
        sub_auth_count = sid_bytes[1]
        identifier     = int.from_bytes(sid_bytes[2:8], "big")
        sub_auths      = struct.unpack(f"<{sub_auth_count}I", sid_bytes[8:])
        return f"S-{revision}-{identifier}-" + "-".join(str(s) for s in sub_auths)
    except Exception:
        return ""


def filetime_to_iso(val) -> Optional[str]:
    if not val:
        return None
    try:
        ft = int(val)
        if ft in (0, 9223372036854775807):
            return None
        epoch = datetime(1601, 1, 1, tzinfo=timezone.utc)
        dt    = epoch + timedelta(microseconds=ft // 10)
        return dt.isoformat()
    except Exception:
        return None


def decode_uac(val) -> dict:
    try:
        uac = int(val)
    except (TypeError, ValueError):
        return {}
    return {name: bool(uac & bit) for bit, name in UAC_FLAGS.items()}


def safe_str(val, default="") -> str:
    if val is None:
        return default
    if isinstance(val, list):
        return str(val[0]) if val else default
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def safe_list(val) -> list:
    if not val:
        return []
    if isinstance(val, list):
        return [str(v) for v in val]
    return [str(val)]


def extract_ou(dn: str) -> str:
    for part in dn.split(","):
        if part.strip().upper().startswith("OU="):
            return part.strip()[3:]
    return ""


def extract_cn(dn: str) -> str:
    for part in dn.split(","):
        if part.strip().upper().startswith("CN="):
            return part.strip()[3:]
    return dn


def get_attr(entry, name: str, default="") -> str:
    try:
        val = getattr(entry, name).value
        if val is None:
            return default
        return safe_str(val, default)
    except Exception:
        return default


def get_attrs(entry, name: str) -> list:
    try:
        val = getattr(entry, name).value
        if val is None:
            return []
        return val if isinstance(val, list) else [val]
    except Exception:
        return []


# ── Connection ──────────────────────────────────────────────────
def connect() -> Connection:
    """
    Connect to AD using Kerberos GSSAPI (Windows) or NTLM fallback.

    On a domain-joined Windows machine: GSSAPI uses your login TGT.
    No credentials needed in .env for Kerberos path.

    NTLM fallback activates if AD_USER + AD_PASSWORD are set in .env.
    """
    host = LDAP_URI.replace("ldap://", "").replace("ldaps://", "").split(":")[0]
    port = 636 if "ldaps://" in LDAP_URI else 389
    use_ssl = "ldaps://" in LDAP_URI

    server = Server(host, port=port, use_ssl=use_ssl, get_info=ALL)

    # ── Try Kerberos GSSAPI first (no password needed on domain PC)
    try:
        conn = Connection(
            server,
            authentication = SASL,
            sasl_mechanism = KERBEROS,
            auto_referrals = False,   # suppress AD cross-domain referrals
            raise_exceptions=True,
        )
        conn.bind()
        log.info("✅  Connected via Kerberos GSSAPI (no password used)")
        return conn
    except Exception as e:
        log.warning("Kerberos GSSAPI failed: %s", e)
        log.warning("Falling back to NTLM...")

    # ── Simple bind fallback (AD_USER + AD_PASSWORD from .env)
    # More reliable than NTLM on Windows without MIT KfW installed
    if not AD_USER or not AD_PASSWORD:
        log.error(
            "Kerberos failed and no AD_USER/AD_PASSWORD in .env.\n"
            "Add to .env (never share in chat):\n"
            "  AD_USER=khatem@sumergedc.local\n"
            "  AD_PASSWORD=your_ad_password_here"
        )
        sys.exit(1)

    try:
        conn = Connection(
            server,
            user             = AD_USER,    # UPN format: user@sumergedc.local
            password         = AD_PASSWORD,
            authentication   = SIMPLE,     # plain LDAP bind — fine on internal DC
            auto_referrals   = False,
            raise_exceptions = True,
        )
        conn.bind()
        log.info("✅  Connected via Simple bind as %s", AD_USER)
        return conn
    except LDAPBindError as e:
        log.error("Bind failed — wrong credentials: %s", e)
        log.error("Verify AD_USER=user@sumergedc.local and AD_PASSWORD in .env")
        sys.exit(1)
    except LDAPSocketOpenError as e:
        log.error("Cannot reach AD at %s — check network/VPN: %s", LDAP_URI, e)
        sys.exit(1)


# ── Paged Search ────────────────────────────────────────────────
def paged_search(
    conn: Connection,
    base: str,
    filter_str: str,
    attributes: list,
) -> list:
    """
    Search with automatic paging (AD default limit = 1000 per query).
    Uses ldap3's built-in paged search generator.
    """
    results = []
    try:
        conn.search(
            search_base   = base,
            search_filter = filter_str,
            search_scope  = SUBTREE,
            attributes    = attributes,
            paged_size    = PAGE_SIZE,
        )
        results.extend(conn.entries)

        # Fetch remaining pages
        cookie = conn.result.get("controls", {}).get(
            "1.2.840.113556.1.4.319", {}
        ).get("value", {}).get("cookie")

        while cookie:
            conn.search(
                search_base   = base,
                search_filter = filter_str,
                search_scope  = SUBTREE,
                attributes    = attributes,
                paged_size    = PAGE_SIZE,
                paged_cookie  = cookie,
            )
            results.extend(conn.entries)
            cookie = conn.result.get("controls", {}).get(
                "1.2.840.113556.1.4.319", {}
            ).get("value", {}).get("cookie")

    except LDAPInsufficientAccessRightsResult:
        log.error("Insufficient access to search '%s'", base)
    except LDAPOperationsErrorResult as e:
        log.error("LDAP operations error: %s", e)

    log.info("Search '%s' → %d objects", filter_str, len(results))
    return results


# ── Users ───────────────────────────────────────────────────────
def fetch_users(conn: Connection) -> list:
    log.info("Fetching all AD users...")
    entries = paged_search(
        conn, BASE_DN,
        "(objectClass=user)",
        [
            "sAMAccountName", "userPrincipalName", "displayName",
            "givenName", "sn", "mail", "department", "title",
            "manager", "memberOf", "distinguishedName",
            "whenCreated", "whenChanged", "lastLogonTimestamp",
            "pwdLastSet", "accountExpires", "userAccountControl",
            "description", "telephoneNumber", "mobile",
            "physicalDeliveryOfficeName", "company", "objectSid",
        ],
    )

    records = []
    for entry in entries:
        dn         = entry.entry_dn or ""
        uac_raw    = get_attr(entry, "userAccountControl", "0")
        uac_flags  = decode_uac(uac_raw)
        sid_raw    = entry.objectSid.raw_values[0] if entry.objectSid.raw_values else b""

        is_service = (
            "OU=ServiceAccounts" in dn
            or "OU=Service Accounts" in dn
            or "svc" in get_attr(entry, "sAMAccountName").lower()
            or "service" in get_attr(entry, "description").lower()
        )

        records.append({
            "sam_account":        get_attr(entry, "sAMAccountName"),
            "upn":                get_attr(entry, "userPrincipalName"),
            "email":              (get_attr(entry, "mail") or get_attr(entry, "userPrincipalName")).lower(),
            "display_name":       get_attr(entry, "displayName"),
            "first_name":         get_attr(entry, "givenName"),
            "last_name":          get_attr(entry, "sn"),
            "department":         get_attr(entry, "department"),
            "title":              get_attr(entry, "title"),
            "company":            get_attr(entry, "company"),
            "office":             get_attr(entry, "physicalDeliveryOfficeName"),
            "phone":              get_attr(entry, "telephoneNumber"),
            "mobile":             get_attr(entry, "mobile"),
            "description":        get_attr(entry, "description"),
            "manager_dn":         get_attr(entry, "manager"),
            "manager_cn":         extract_cn(get_attr(entry, "manager")),
            "member_of_dns":      get_attrs(entry, "memberOf"),
            "member_of_names":    [extract_cn(g) for g in get_attrs(entry, "memberOf")],
            "distinguished_name": dn,
            "ou":                 extract_ou(dn),
            "object_sid":         decode_sid(sid_raw),
            "enabled":            not uac_flags.get("ACCOUNTDISABLE", False),
            "locked_out":         uac_flags.get("LOCKOUT", False),
            "pwd_no_expire":      uac_flags.get("DONT_EXPIRE_PASSWD", False),
            "is_service_account": is_service,
            "created":            get_attr(entry, "whenCreated"),
            "modified":           get_attr(entry, "whenChanged"),
            "last_logon":         filetime_to_iso(get_attr(entry, "lastLogonTimestamp")),
            "pwd_last_set":       filetime_to_iso(get_attr(entry, "pwdLastSet")),
            "_ingested_at":       datetime.utcnow().isoformat(),
        })

    log.info(
        "Users: %d total | %d service accounts | %d disabled",
        len(records),
        sum(1 for u in records if u["is_service_account"]),
        sum(1 for u in records if not u["enabled"]),
    )
    return records


def lookup_user(conn: Connection, sam: str):
    entries = paged_search(
        conn, BASE_DN,
        f"(&(objectClass=user)(sAMAccountName={sam}))",
        ["*"],
    )
    if not entries:
        log.error("No user found: %s", sam)
        return
    e = entries[0]
    uac = decode_uac(get_attr(e, "userAccountControl", "0"))
    print(f"\n{'═'*60}")
    print(f"  USER: {get_attr(e, 'displayName')}  ({sam})")
    print(f"{'═'*60}")
    print(f"  DN:          {e.entry_dn}")
    print(f"  UPN:         {get_attr(e, 'userPrincipalName')}")
    print(f"  Email:       {get_attr(e, 'mail')}")
    print(f"  Department:  {get_attr(e, 'department')}")
    print(f"  Title:       {get_attr(e, 'title')}")
    print(f"  Manager:     {extract_cn(get_attr(e, 'manager'))}")
    print(f"  Last Logon:  {filetime_to_iso(get_attr(e, 'lastLogonTimestamp'))}")
    print(f"  Enabled:     {not uac.get('ACCOUNTDISABLE', False)}")
    print(f"  Pwd Expires: {not uac.get('DONT_EXPIRE_PASSWD', False)}")
    print(f"\n  Groups:")
    for g in get_attrs(e, "memberOf"):
        print(f"    • {extract_cn(g)}")
    print()


# ── Groups ──────────────────────────────────────────────────────
def fetch_groups(conn: Connection) -> list:
    log.info("Fetching all AD groups...")
    entries = paged_search(
        conn, BASE_DN,
        "(objectClass=group)",
        ["cn", "distinguishedName", "description", "groupType",
         "member", "memberOf", "whenCreated", "objectSid"],
    )
    GROUP_TYPES = {
        -2147483646: "Global Security",
        -2147483644: "Domain Local Security",
        -2147483640: "Universal Security",
        2:           "Global Distribution",
        4:           "Domain Local Distribution",
        8:           "Universal Distribution",
    }
    records = []
    for entry in entries:
        dn  = entry.entry_dn or ""
        try:
            gt = int(get_attr(entry, "groupType", "0"))
        except ValueError:
            gt = 0
        records.append({
            "group_name":         get_attr(entry, "cn"),
            "distinguished_name": dn,
            "group_type":         GROUP_TYPES.get(gt, str(gt)),
            "description":        get_attr(entry, "description"),
            "member_names":       [extract_cn(m) for m in get_attrs(entry, "member")],
            "member_count":       len(get_attrs(entry, "member")),
            "parent_groups":      [extract_cn(g) for g in get_attrs(entry, "memberOf")],
            "ou":                 extract_ou(dn),
            "_ingested_at":       datetime.utcnow().isoformat(),
        })
    return records


def lookup_group(conn: Connection, group_cn: str):
    entries = paged_search(
        conn, BASE_DN,
        f"(&(objectClass=group)(cn={group_cn}))",
        ["cn", "distinguishedName", "description", "member", "groupType"],
    )
    if not entries:
        log.error("No group found: %s", group_cn)
        return
    e       = entries[0]
    members = get_attrs(e, "member")
    print(f"\n{'═'*60}")
    print(f"  GROUP: {get_attr(e, 'cn')}")
    print(f"{'═'*60}")
    print(f"  DN:          {e.entry_dn}")
    print(f"  Description: {get_attr(e, 'description')}")
    print(f"  Members ({len(members)}):")
    for m in members:
        print(f"    • {extract_cn(m)}")

    # Nested resolution using LDAP_MATCHING_RULE_IN_CHAIN
    print(f"\n  Resolving nested (transitive) members...")
    nested = paged_search(
        conn, BASE_DN,
        f"(memberOf:1.2.840.113556.1.4.1941:={e.entry_dn})",
        ["sAMAccountName", "displayName", "mail"],
    )
    if nested:
        print(f"  All transitive members ({len(nested)}):")
        for n in nested:
            print(f"    ↳ {get_attr(n, 'displayName')} ({get_attr(n, 'sAMAccountName')})")
    print()


# ── OUs ─────────────────────────────────────────────────────────
def fetch_ous(conn: Connection) -> list:
    log.info("Fetching full OU structure...")
    entries = paged_search(
        conn, BASE_DN,
        "(objectClass=organizationalUnit)",
        ["ou", "distinguishedName", "description", "whenCreated", "whenChanged"],
    )
    records = []
    for entry in entries:
        dn    = entry.entry_dn or ""
        parts = dn.split(",")
        depth = dn.upper().count("OU=")
        records.append({
            "ou_name":            get_attr(entry, "ou"),
            "distinguished_name": dn,
            "description":        get_attr(entry, "description"),
            "depth":              depth,
            "parent_dn":          ",".join(parts[1:]) if len(parts) > 1 else BASE_DN,
            "path":               " > ".join(
                reversed([p[3:] for p in parts if p.upper().startswith("OU=")])
            ),
            "created":            get_attr(entry, "whenCreated"),
            "_ingested_at":       datetime.utcnow().isoformat(),
        })
    records.sort(key=lambda r: r["depth"])
    return records


# ── Computers ───────────────────────────────────────────────────
def fetch_computers(conn: Connection) -> list:
    log.info("Fetching computer objects...")
    entries = paged_search(
        conn, BASE_DN,
        "(objectClass=computer)",
        ["cn", "dNSHostName", "operatingSystem", "operatingSystemVersion",
         "distinguishedName", "whenCreated", "whenChanged",
         "lastLogonTimestamp", "userAccountControl", "description", "location"],
    )
    records = []
    for entry in entries:
        dn  = entry.entry_dn or ""
        uac = decode_uac(get_attr(entry, "userAccountControl", "0"))
        records.append({
            "computer_name":      get_attr(entry, "cn"),
            "dns_hostname":       get_attr(entry, "dNSHostName"),
            "os":                 get_attr(entry, "operatingSystem"),
            "os_version":         get_attr(entry, "operatingSystemVersion"),
            "description":        get_attr(entry, "description"),
            "location":           get_attr(entry, "location"),
            "ou":                 extract_ou(dn),
            "distinguished_name": dn,
            "enabled":            not uac.get("ACCOUNTDISABLE", False),
            "created":            get_attr(entry, "whenCreated"),
            "last_logon":         filetime_to_iso(get_attr(entry, "lastLogonTimestamp")),
            "_ingested_at":       datetime.utcnow().isoformat(),
        })
    log.info(
        "Computers: %d total | %d enabled",
        len(records),
        sum(1 for c in records if c["enabled"]),
    )
    return records


# ── Kafka ────────────────────────────────────────────────────────
def publish(producer: Producer, topic: str, key_field: str, records: list):
    for r in records:
        producer.produce(
            topic    = topic,
            key      = str(r.get(key_field, "unknown")),
            value    = json.dumps(r, default=str),
            callback = lambda err, _: log.error("Kafka: %s", err) if err else None,
        )
        producer.poll(0)
    producer.flush()
    log.info("✅  Published %d → %s", len(records), topic)


# ── Main ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="AD → Kafka (Kerberos/NTLM, Windows-native)")
    parser.add_argument("--mode", nargs="+",
                        choices=["users", "groups", "ous", "computers", "all"],
                        default=["all"])
    parser.add_argument("--lookup",   metavar="SAM",       help="Print single user details")
    parser.add_argument("--group",    metavar="GROUP_CN",  help="Print group + members")
    parser.add_argument("--no-kafka", action="store_true", help="Dry run — skip Kafka")
    args = parser.parse_args()

    conn     = connect()
    producer = None if args.no_kafka else Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "ad-producer",
        "acks": "all",
    })

    if args.lookup:
        lookup_user(conn, args.lookup)
        conn.unbind()
        return

    if args.group:
        lookup_group(conn, args.group)
        conn.unbind()
        return

    modes = args.mode
    if "all" in modes:
        modes = ["users", "groups", "ous", "computers"]

    if "users" in modes:
        data = fetch_users(conn)
        if producer:
            publish(producer, TOPIC_USERS, "sam_account", data)
        else:
            print(json.dumps(data[:2], indent=2, default=str))

    if "groups" in modes:
        data = fetch_groups(conn)
        if producer:
            publish(producer, TOPIC_GROUPS, "group_name", data)

    if "ous" in modes:
        data = fetch_ous(conn)
        if producer:
            publish(producer, TOPIC_OUS, "distinguished_name", data)
        print("\nOU Tree:")
        for ou in data:
            indent = "  " * (ou["depth"] - 1)
            prefix = "└─" if ou["depth"] > 1 else ""
            print(f"{indent}{prefix}[OU] {ou['ou_name']}"
                  + (f"  — {ou['description']}" if ou["description"] else ""))

    if "computers" in modes:
        data = fetch_computers(conn)
        if producer:
            publish(producer, TOPIC_COMPUTERS, "computer_name", data)

    conn.unbind()
    log.info("🎉  AD sync complete")


if __name__ == "__main__":
    main()
